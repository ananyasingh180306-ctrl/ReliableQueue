import { EventEmitter } from 'node:events';
import { setTimeout as sleep } from 'node:timers/promises';
import { taskRegistry } from './tasks/registry.js';
import { createStepRunner } from './idempotency.js';

export class WorkerPool extends EventEmitter {
  /**
   * @param {import('./queue.js').JobQueue} queue
   * @param {object} [options]
   * @param {number} [options.concurrency=2] - Number of concurrent jobs to process
   * @param {number} [options.pollIntervalMs=500] - Polling interval when idle
   * @param {number} [options.visibilityTimeoutMs=15000] - Job lease visibility timeout
   * @param {string} [options.workerId] - Unique worker ID
   */
  constructor(queue, options = {}) {
    super();
    this.queue = queue;
    this.concurrency = options.concurrency || 2;
    this.pollIntervalMs = options.pollIntervalMs || 500;
    this.visibilityTimeoutMs = options.visibilityTimeoutMs || 15000;
    this.workerId = options.workerId || `worker-${process.pid}-${Math.random().toString(36).slice(2, 6)}`;
    
    this.running = false;
    this.activeJobsCount = 0;
    this.activeHeartbeats = new Map(); // jobId -> intervalTimer
  }

  /**
   * Starts the worker polling loops.
   */
  async start() {
    if (this.running) return;
    this.running = true;
    this.emit('started', { workerId: this.workerId, concurrency: this.concurrency });

    // Spawn concurrent loop runners
    for (let i = 0; i < this.concurrency; i++) {
      this._runSlot(i);
    }
  }

  /**
   * Gracefully shuts down the worker pool.
   */
  async stop() {
    this.running = false;
    this.emit('stopping', { workerId: this.workerId });

    // Wait until all in-flight jobs finish (up to 10s)
    const start = Date.now();
    while (this.activeJobsCount > 0 && Date.now() - start < 10000) {
      await sleep(100);
    }

    // Clear any remaining heartbeat timers
    for (const [jobId, timer] of this.activeHeartbeats) {
      clearInterval(timer);
    }
    this.activeHeartbeats.clear();
    this.emit('stopped', { workerId: this.workerId });
  }

  /**
   * Dedicated slot runner that polls and executes jobs.
   */
  async _runSlot(slotIndex) {
    while (this.running) {
      try {
        const job = this.queue.leaseNextJob(this.workerId, this.visibilityTimeoutMs);
        if (!job) {
          await sleep(this.pollIntervalMs);
          continue;
        }

        this.activeJobsCount++;
        this.emit('job:leased', { workerId: this.workerId, slotIndex, job });

        await this._executeJob(job);
      } catch (err) {
        this.emit('error', { workerId: this.workerId, slotIndex, error: err });
        await sleep(this.pollIntervalMs);
      } finally {
        this.activeJobsCount = Math.max(0, this.activeJobsCount - 1);
      }
    }
  }

  /**
   * Executes a single job, managing heartbeats and idempotency.
   */
  async _executeJob(job) {
    const handler = taskRegistry[job.type];

    // Setup periodic lease heartbeat (renew every 40% of visibility timeout)
    const heartbeatIntervalMs = Math.max(1000, Math.floor(this.visibilityTimeoutMs * 0.4));
    const heartbeatTimer = setInterval(() => {
      try {
        const renewed = this.queue.heartbeat(job.id, this.workerId, this.visibilityTimeoutMs);
        if (renewed) {
          this.emit('job:heartbeat', { workerId: this.workerId, jobId: job.id });
        }
      } catch (err) {
        // Ignored or logged
      }
    }, heartbeatIntervalMs);

    this.activeHeartbeats.set(job.id, heartbeatTimer);

    // Step runner for idempotency
    const step = createStepRunner(this.queue.db, job.id, {
      onStepSkip: (stepName, result) => {
        this.emit('job:step_skipped', {
          jobId: job.id,
          stepName,
          result,
          message: `[Idempotency] Step '${stepName}' was already executed. Skipped duplicate side effect.`
        });
      },
      onStepComplete: (stepName, result) => {
        this.emit('job:step_completed', {
          jobId: job.id,
          stepName,
          result
        });
      }
    });

    const context = {
      step,
      heartbeat: () => this.queue.heartbeat(job.id, this.workerId, this.visibilityTimeoutMs),
      log: (msg) => this.emit('job:log', { jobId: job.id, message: msg })
    };

    try {
      if (!handler) {
        throw new Error(`No task handler registered for job type '${job.type}'`);
      }

      const result = await handler(job, context);

      // Stop heartbeat and mark complete
      clearInterval(heartbeatTimer);
      this.activeHeartbeats.delete(job.id);

      this.queue.completeJob(job.id, this.workerId);
      this.emit('job:completed', { workerId: this.workerId, job, result });
    } catch (err) {
      // Stop heartbeat and handle retry or DLQ
      clearInterval(heartbeatTimer);
      this.activeHeartbeats.delete(job.id);

      const failResult = this.queue.failJob(job.id, this.workerId, err);
      this.emit('job:failed', {
        workerId: this.workerId,
        job,
        error: err.message,
        status: failResult.status,
        retryInMs: failResult.retryInMs
      });
    }
  }
}
