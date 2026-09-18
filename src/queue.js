import { createDatabase } from './db.js';
import { getJobSteps } from './idempotency.js';

export class JobQueue {
  /**
   * @param {object} [options]
   * @param {string|import('node:sqlite').DatabaseSync} [options.db='./data/queue.db']
   */
  constructor(options = {}) {
    if (typeof options.db === 'object' && options.db !== null) {
      this.db = options.db;
    } else {
      this.db = createDatabase(options.db);
    }
  }

  /**
   * Enqueues a new job into the durable queue.
   * @param {object} params
   * @param {string} params.type - Job task type (e.g. 'email', 'order_fulfillment')
   * @param {object} [params.payload={}] - JSON serializable job payload
   * @param {number} [params.priority=0] - Higher number = higher priority
   * @param {number} [params.delayMs=0] - Delay in ms before job becomes eligible
   * @param {number} [params.runAt] - Absolute timestamp (epoch ms) to run at
   * @param {number} [params.maxAttempts=3] - Maximum retry attempts
   * @param {number} [params.backoffBaseMs=1000] - Base delay for exponential backoff
   * @param {string} [params.idempotencyKey] - Unique key to prevent duplicate job creation
   * @param {number[]} [params.dependsOn=[]] - Array of parent job IDs that must complete first
   * @returns {object} The created or existing job record
   */
  enqueue({
    type,
    payload = {},
    priority = 0,
    delayMs = 0,
    runAt = null,
    maxAttempts = 3,
    backoffBaseMs = 1000,
    idempotencyKey = null,
    dependsOn = []
  }) {
    if (!type) {
      throw new Error('Job type is required');
    }

    const now = Date.now();
    const scheduledRunAt = runAt !== null ? runAt : now + Math.max(0, delayMs);

    // Check for idempotency key collision
    if (idempotencyKey) {
      const existing = this.db.prepare(
        'SELECT * FROM jobs WHERE idempotency_key = ?'
      ).get(idempotencyKey);

      if (existing) {
        return {
          ...this._formatJob(existing),
          deduplicated: true
        };
      }
    }

    this.db.exec('BEGIN IMMEDIATE;');
    try {
      const stmt = this.db.prepare(`
        INSERT INTO jobs (
          type, payload, status, priority, run_at,
          attempts, max_attempts, backoff_base_ms,
          idempotency_key, created_at, updated_at
        ) VALUES (?, ?, 'queued', ?, ?, 0, ?, ?, ?, ?, ?)
        RETURNING *;
      `);

      const row = stmt.get(
        type,
        JSON.stringify(payload),
        priority,
        scheduledRunAt,
        maxAttempts,
        backoffBaseMs,
        idempotencyKey || null,
        now,
        now
      );

      // Register parent dependencies
      if (Array.isArray(dependsOn) && dependsOn.length > 0) {
        const depStmt = this.db.prepare(`
          INSERT INTO job_dependencies (parent_job_id, child_job_id)
          VALUES (?, ?);
        `);
        for (const parentId of dependsOn) {
          depStmt.run(parentId, row.id);
        }
      }

      this.db.exec('COMMIT;');
      return this._formatJob(row);
    } catch (err) {
      this.db.exec('ROLLBACK;');
      // Handle race condition on unique idempotency_key
      if (idempotencyKey && err.message?.includes('UNIQUE constraint failed')) {
        const existing = this.db.prepare(
          'SELECT * FROM jobs WHERE idempotency_key = ?'
        ).get(idempotencyKey);
        if (existing) {
          return { ...this._formatJob(existing), deduplicated: true };
        }
      }
      throw err;
    }
  }

  /**
   * Atomically leases the next highest-priority eligible job.
   * Also recovers any expired leases from crashed workers.
   * @param {string} workerId - Unique identifier of the worker
   * @param {number} [visibilityTimeoutMs=30000] - Lease duration in ms
   * @returns {object|null} Leased job or null if no job is ready
   */
  leaseNextJob(workerId, visibilityTimeoutMs = 30000) {
    const now = Date.now();
    const leaseExpiresAt = now + visibilityTimeoutMs;

    this.db.exec('BEGIN IMMEDIATE;');
    try {
      // Find candidate job:
      // 1. Either queued and run_at <= now
      // 2. Or leased but lease_expires_at <= now (crashed worker lease recovery)
      // AND all parent dependencies are completed
      const candidateStmt = this.db.prepare(`
        SELECT j.id, j.status, j.attempts, j.max_attempts, j.worker_id
        FROM jobs j
        WHERE (
          (j.status = 'queued' AND j.run_at <= ?)
          OR
          (j.status = 'leased' AND j.lease_expires_at <= ?)
        )
        AND NOT EXISTS (
          SELECT 1 FROM job_dependencies jd
          JOIN jobs p ON jd.parent_job_id = p.id
          WHERE jd.child_job_id = j.id AND p.status != 'completed'
        )
        ORDER BY j.priority DESC, j.run_at ASC, j.id ASC
        LIMIT 1;
      `);

      const candidate = candidateStmt.get(now, now);
      if (!candidate) {
        this.db.exec('COMMIT;');
        return null;
      }

      // If leasing an expired job, record that this was a crash recovery
      const wasExpiredLease = candidate.status === 'leased';

      // Increment attempt count on acquisition
      const updateStmt = this.db.prepare(`
        UPDATE jobs
        SET status = 'leased',
            worker_id = ?,
            leased_at = ?,
            lease_expires_at = ?,
            attempts = attempts + 1,
            updated_at = ?
        WHERE id = ?
        RETURNING *;
      `);

      const leasedJob = updateStmt.get(
        workerId,
        now,
        leaseExpiresAt,
        now,
        candidate.id
      );

      this.db.exec('COMMIT;');

      const formatted = this._formatJob(leasedJob);
      if (wasExpiredLease) {
        formatted.recoveredFromCrash = true;
      }
      return formatted;
    } catch (err) {
      this.db.exec('ROLLBACK;');
      throw err;
    }
  }

  /**
   * Heartbeat to extend visibility lease for long-running jobs.
   * @param {number} jobId
   * @param {string} workerId
   * @param {number} [extensionMs=30000]
   * @returns {boolean} True if lease was successfully extended
   */
  heartbeat(jobId, workerId, extensionMs = 30000) {
    const newExpiry = Date.now() + extensionMs;
    const stmt = this.db.prepare(`
      UPDATE jobs
      SET lease_expires_at = ?,
          updated_at = ?
      WHERE id = ? AND worker_id = ? AND status = 'leased';
    `);
    const result = stmt.run(newExpiry, Date.now(), jobId, workerId);
    return result.changes > 0;
  }

  /**
   * Marks a job as completed successfully.
   * @param {number} jobId
   * @param {string} workerId
   * @returns {boolean}
   */
  completeJob(jobId, workerId) {
    const now = Date.now();
    const stmt = this.db.prepare(`
      UPDATE jobs
      SET status = 'completed',
          worker_id = NULL,
          lease_expires_at = NULL,
          completed_at = ?,
          updated_at = ?
      WHERE id = ? AND worker_id = ? AND status = 'leased';
    `);
    const result = stmt.run(now, now, jobId, workerId);
    return result.changes > 0;
  }

  /**
   * Handles job failure: schedules exponential backoff retry or transitions to dead-letter queue.
   * @param {number} jobId
   * @param {string} workerId
   * @param {Error|string} error
   * @returns {{ status: 'queued'|'dead_letter', attempts: number, retryInMs?: number }}
   */
  failJob(jobId, workerId, error) {
    const now = Date.now();
    const errorMessage = typeof error === 'string' ? error : (error?.stack || error?.message || 'Unknown error');

    this.db.exec('BEGIN IMMEDIATE;');
    try {
      const job = this.db.prepare('SELECT * FROM jobs WHERE id = ?').get(jobId);
      if (!job) {
        this.db.exec('COMMIT;');
        throw new Error(`Job ${jobId} not found`);
      }

      if (job.attempts >= job.max_attempts) {
        // Exceeded max retries -> Move to Dead-Letter Queue
        const dlqStmt = this.db.prepare(`
          UPDATE jobs
          SET status = 'dead_letter',
              worker_id = NULL,
              lease_expires_at = NULL,
              last_error = ?,
              updated_at = ?
          WHERE id = ?;
        `);
        dlqStmt.run(errorMessage, now, jobId);
        this.db.exec('COMMIT;');
        return { status: 'dead_letter', attempts: job.attempts };
      } else {
        // Exponential backoff: base * 2^(attempts-1) + jitter
        const jitter = Math.floor(Math.random() * 150);
        const backoffMs = Math.min(
          job.backoff_base_ms * Math.pow(2, job.attempts - 1) + jitter,
          300000 // 5 minutes max ceiling
        );
        const nextRunAt = now + backoffMs;

        const retryStmt = this.db.prepare(`
          UPDATE jobs
          SET status = 'queued',
              run_at = ?,
              worker_id = NULL,
              lease_expires_at = NULL,
              last_error = ?,
              updated_at = ?
          WHERE id = ?;
        `);
        retryStmt.run(nextRunAt, errorMessage, now, jobId);
        this.db.exec('COMMIT;');
        return { status: 'queued', attempts: job.attempts, retryInMs: backoffMs };
      }
    } catch (err) {
      this.db.exec('ROLLBACK;');
      throw err;
    }
  }

  /**
   * Replays a dead-letter job by resetting its status to 'queued'.
   * @param {number} jobId
   * @returns {boolean}
   */
  replayDeadLetterJob(jobId) {
    const now = Date.now();
    const stmt = this.db.prepare(`
      UPDATE jobs
      SET status = 'queued',
          attempts = 0,
          run_at = ?,
          worker_id = NULL,
          lease_expires_at = NULL,
          last_error = NULL,
          updated_at = ?
      WHERE id = ? AND status = 'dead_letter';
    `);
    const res = stmt.run(now, now, jobId);
    return res.changes > 0;
  }

  /**
   * Replays all jobs currently in the dead-letter queue.
   * @returns {number} Count of replayed jobs
   */
  replayAllDeadLetterJobs() {
    const now = Date.now();
    const stmt = this.db.prepare(`
      UPDATE jobs
      SET status = 'queued',
          attempts = 0,
          run_at = ?,
          worker_id = NULL,
          lease_expires_at = NULL,
          last_error = NULL,
          updated_at = ?
      WHERE status = 'dead_letter';
    `);
    const res = stmt.run(now, now);
    return res.changes;
  }

  /**
   * Retrieves high-level queue metrics.
   * @returns {{ queued: number, leased: number, completed: number, dead_letter: number, total: number }}
   */
  getStats() {
    const rows = this.db.prepare(`
      SELECT status, COUNT(*) as count 
      FROM jobs 
      GROUP BY status;
    `).all();

    const stats = {
      queued: 0,
      leased: 0,
      completed: 0,
      dead_letter: 0,
      total: 0
    };

    for (const row of rows) {
      if (stats[row.status] !== undefined) {
        stats[row.status] = Number(row.count);
      }
      stats.total += Number(row.count);
    }
    return stats;
  }

  /**
   * Returns all currently in-flight (leased) jobs with remaining lease time.
   * @returns {Array<object>}
   */
  getInFlightJobs() {
    const now = Date.now();
    const rows = this.db.prepare(`
      SELECT * FROM jobs 
      WHERE status = 'leased' 
      ORDER BY lease_expires_at ASC;
    `).all();

    return rows.map(r => {
      const job = this._formatJob(r);
      job.remainingLeaseMs = Math.max(0, (r.lease_expires_at || 0) - now);
      return job;
    });
  }

  /**
   * Retrieves dead-letter queue entries.
   * @param {number} [limit=50]
   * @returns {Array<object>}
   */
  getDeadLetterJobs(limit = 50) {
    const rows = this.db.prepare(`
      SELECT * FROM jobs 
      WHERE status = 'dead_letter' 
      ORDER BY updated_at DESC 
      LIMIT ?;
    `).all(limit);

    return rows.map(r => this._formatJob(r));
  }

  /**
   * Retrieves recent jobs regardless of status for dashboard overview.
   * @param {number} [limit=50]
   * @returns {Array<object>}
   */
  getRecentJobs(limit = 50) {
    const rows = this.db.prepare(`
      SELECT * FROM jobs 
      ORDER BY id DESC 
      LIMIT ?;
    `).all(limit);

    return rows.map(r => this._formatJob(r));
  }

  /**
   * Retrieves a job by ID along with its steps and dependencies.
   * @param {number} jobId
   * @returns {object|null}
   */
  getJobById(jobId) {
    const row = this.db.prepare('SELECT * FROM jobs WHERE id = ?').get(jobId);
    if (!row) return null;

    const job = this._formatJob(row);
    job.steps = getJobSteps(this.db, jobId);

    const deps = this.db.prepare(`
      SELECT p.id, p.type, p.status 
      FROM job_dependencies jd
      JOIN jobs p ON jd.parent_job_id = p.id
      WHERE jd.child_job_id = ?;
    `).all(jobId);
    job.parentDependencies = deps;

    return job;
  }

  /**
   * Internal formatter for database job rows.
   */
  _formatJob(row) {
    let payload = {};
    try {
      payload = JSON.parse(row.payload);
    } catch {
      payload = row.payload;
    }

    return {
      id: row.id,
      type: row.type,
      payload,
      status: row.status,
      priority: row.priority,
      runAt: row.run_at,
      attempts: row.attempts,
      maxAttempts: row.max_attempts,
      backoffBaseMs: row.backoff_base_ms,
      workerId: row.worker_id,
      leasedAt: row.leased_at,
      leaseExpiresAt: row.lease_expires_at,
      lastError: row.last_error,
      idempotencyKey: row.idempotency_key,
      createdAt: row.created_at,
      updatedAt: row.updated_at,
      completedAt: row.completed_at
    };
  }
}
