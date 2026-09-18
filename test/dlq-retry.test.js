import test from 'node:test';
import assert from 'node:assert/strict';
import { JobQueue } from '../src/queue.js';
import { createDatabase } from '../src/db.js';

test('Exponential Backoff, Dead-Letter Queue and Replay', async (t) => {
  const db = createDatabase(':memory:');
  const queue = new JobQueue({ db });

  await t.test('Failing job retries with backoff and moves to DLQ on max attempts', () => {
    const job = queue.enqueue({
      type: 'payment',
      payload: { customer: 'alice' },
      maxAttempts: 3,
      backoffBaseMs: 500
    });

    // Attempt 1: Worker 1 leases and fails
    const l1 = queue.leaseNextJob('worker-1', 5000);
    assert.equal(l1.id, job.id);
    assert.equal(l1.attempts, 1);

    const f1 = queue.failJob(l1.id, 'worker-1', new Error('Gateway timeout'));
    assert.equal(f1.status, 'queued');
    assert.equal(f1.attempts, 1);
    assert.ok(f1.retryInMs >= 500, 'Backoff should be at least base delay');

    // Attempt 2: Fast forward run_at
    db.prepare('UPDATE jobs SET run_at = ? WHERE id = ?').run(Date.now() - 100, job.id);
    const l2 = queue.leaseNextJob('worker-2', 5000);
    assert.equal(l2.attempts, 2);

    const f2 = queue.failJob(l2.id, 'worker-2', new Error('Gateway still down'));
    assert.equal(f2.status, 'queued');
    assert.equal(f2.attempts, 2);
    assert.ok(f2.retryInMs >= 1000, 'Backoff should be at least base * 2^1 = 1000ms');

    // Attempt 3: Fast forward run_at
    db.prepare('UPDATE jobs SET run_at = ? WHERE id = ?').run(Date.now() - 100, job.id);
    const l3 = queue.leaseNextJob('worker-3', 5000);
    assert.equal(l3.attempts, 3);

    const f3 = queue.failJob(l3.id, 'worker-3', new Error('Gateway permanently rejected'));
    // Since attempts reached max_attempts (3), it MUST move to dead_letter!
    assert.equal(f3.status, 'dead_letter');

    // Verify DLQ state
    const dlqJobs = queue.getDeadLetterJobs();
    assert.equal(dlqJobs.length, 1);
    assert.equal(dlqJobs[0].id, job.id);
    assert.equal(dlqJobs[0].status, 'dead_letter');
    assert.ok(dlqJobs[0].lastError.includes('Gateway permanently rejected'));

    // Stats reflect DLQ
    const stats = queue.getStats();
    assert.equal(stats.dead_letter, 1);
    assert.equal(stats.queued, 0);
  });

  await t.test('Replaying a dead-letter job resets it for execution', () => {
    const dlqJobs = queue.getDeadLetterJobs();
    assert.equal(dlqJobs.length, 1);
    const jobId = dlqJobs[0].id;

    // Replay the job
    const success = queue.replayDeadLetterJob(jobId);
    assert.equal(success, true);

    // Verify state reset
    const replayed = queue.getJobById(jobId);
    assert.equal(replayed.status, 'queued');
    assert.equal(replayed.attempts, 0);
    assert.equal(replayed.lastError, null);

    const stats = queue.getStats();
    assert.equal(stats.dead_letter, 0);
    assert.equal(stats.queued, 1);

    // Next worker can immediately lease it
    const leased = queue.leaseNextJob('worker-replayer', 5000);
    assert.ok(leased);
    assert.equal(leased.id, jobId);
    assert.equal(leased.attempts, 1);
  });
});
