import test from 'node:test';
import assert from 'node:assert/strict';
import { JobQueue } from '../src/queue.js';
import { createDatabase } from '../src/db.js';
import { setTimeout as sleep } from 'node:timers/promises';

test('Priority Scheduling, Delayed Execution and DAG Dependencies', async (t) => {
  const db = createDatabase(':memory:');
  const queue = new JobQueue({ db });

  await t.test('Priority ordering: higher priority job is leased first', () => {
    const low = queue.enqueue({ type: 'task', priority: 1, payload: { name: 'low' } });
    const high = queue.enqueue({ type: 'task', priority: 10, payload: { name: 'high' } });
    const medium = queue.enqueue({ type: 'task', priority: 5, payload: { name: 'medium' } });

    const first = queue.leaseNextJob('worker-p');
    assert.equal(first.id, high.id, 'Priority 10 should be leased first');

    const second = queue.leaseNextJob('worker-p');
    assert.equal(second.id, medium.id, 'Priority 5 should be leased second');

    const third = queue.leaseNextJob('worker-p');
    assert.equal(third.id, low.id, 'Priority 1 should be leased last');
  });

  await t.test('Delayed execution: job is invisible until run_at arrives', async () => {
    const delayedJob = queue.enqueue({
      type: 'task',
      delayMs: 300,
      payload: { name: 'delayed' }
    });

    // Should not be available immediately
    const immediate = queue.leaseNextJob('worker-d');
    assert.equal(immediate, null, 'Delayed job should not be leased before delay expires');

    // Wait for delay
    await sleep(350);

    // Now eligible
    const ready = queue.leaseNextJob('worker-d');
    assert.ok(ready);
    assert.equal(ready.id, delayedJob.id);
  });

  await t.test('DAG Dependencies: Child job only runs AFTER parent completes', () => {
    // Parent Job A
    const parentA = queue.enqueue({ type: 'task', payload: { step: 'A' } });

    // Child Job B (depends on Parent A)
    const childB = queue.enqueue({
      type: 'task',
      payload: { step: 'B' },
      dependsOn: [parentA.id]
    });

    // Try to lease: only Parent A should be eligible! Child B must wait.
    const leased1 = queue.leaseNextJob('worker-dag');
    assert.equal(leased1.id, parentA.id, 'Parent A must be leased');

    // With Parent A in-flight, Child B still cannot be leased
    const leased2 = queue.leaseNextJob('worker-dag');
    assert.equal(leased2, null, 'Child B must not be leased while parent is in-flight');

    // Complete Parent A
    const completed = queue.completeJob(parentA.id, 'worker-dag');
    assert.equal(completed, true);

    // Now Child B is unblocked and can be leased
    const leasedChild = queue.leaseNextJob('worker-dag');
    assert.ok(leasedChild, 'Child B should now be eligible');
    assert.equal(leasedChild.id, childB.id);
  });
});
