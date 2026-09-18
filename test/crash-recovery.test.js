import test from 'node:test';
import assert from 'node:assert/strict';
import { JobQueue } from '../src/queue.js';
import { createDatabase } from '../src/db.js';
import { setTimeout as sleep } from 'node:timers/promises';

test('Crash Recovery and Visibility Timeout Lease Pattern', async (t) => {
  const db = createDatabase(':memory:');
  const queue = new JobQueue({ db });

  await t.test('Worker 1 leases a job; Worker 2 cannot lease it while lease is active', () => {
    const job = queue.enqueue({ type: 'email', payload: { to: 'test@example.com' } });
    assert.equal(job.status, 'queued');

    // Worker 1 claims job with 2-second lease
    const leasedJob1 = queue.leaseNextJob('worker-alpha', 2000);
    assert.ok(leasedJob1, 'Worker 1 should receive job');
    assert.equal(leasedJob1.id, job.id);
    assert.equal(leasedJob1.workerId, 'worker-alpha');

    // Worker 2 attempts to claim job immediately
    const leasedJob2 = queue.leaseNextJob('worker-beta', 2000);
    assert.equal(leasedJob2, null, 'Worker 2 must not be able to steal an active lease');
  });

  await t.test('Crashed worker: lease expires and Worker 2 recovers the job', async () => {
    // We create a job with a short 300ms visibility lease
    const job = queue.enqueue({ type: 'email', payload: { to: 'worker-crash@example.com' } });
    
    // Worker 1 leases the job then "crashes" (never heartbeats or completes it)
    const leased1 = queue.leaseNextJob('crashed-worker', 300);
    assert.equal(leased1.id, job.id);
    assert.equal(leased1.attempts, 1);

    // Immediate attempt by Worker 2 fails
    assert.equal(queue.leaseNextJob('survivor-worker', 1000), null);

    // Wait for visibility timeout (350ms)
    await sleep(350);

    // Worker 2 now polls and recovers the job from the dead worker!
    const recovered = queue.leaseNextJob('survivor-worker', 1000);
    assert.ok(recovered, 'Survivor worker should have recovered expired lease');
    assert.equal(recovered.id, job.id);
    assert.equal(recovered.workerId, 'survivor-worker');
    assert.equal(recovered.attempts, 2, 'Attempts count should have incremented');
    assert.equal(recovered.recoveredFromCrash, true);

    // Survivor worker successfully completes the job
    const completed = queue.completeJob(recovered.id, 'survivor-worker');
    assert.equal(completed, true);

    const finalJob = queue.getJobById(job.id);
    assert.equal(finalJob.status, 'completed');
  });
});
