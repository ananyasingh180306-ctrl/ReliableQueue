import test from 'node:test';
import assert from 'node:assert/strict';
import { JobQueue } from '../src/queue.js';
import { createDatabase } from '../src/db.js';
import { createStepRunner } from '../src/idempotency.js';

test('Idempotency: Submission Deduplication and Multi-step Crash Resilience', async (t) => {
  await t.test('Submission level idempotency: duplicate keys do not create extra jobs', () => {
    const db = createDatabase(':memory:');
    const queue = new JobQueue({ db });

    const job1 = queue.enqueue({
      type: 'charge',
      payload: { amount: 100 },
      idempotencyKey: 'tx_req_9999'
    });
    assert.equal(job1.idempotencyKey, 'tx_req_9999');
    assert.equal(job1.deduplicated, undefined);

    const job2 = queue.enqueue({
      type: 'charge',
      payload: { amount: 100 },
      idempotencyKey: 'tx_req_9999'
    });
    assert.equal(job2.id, job1.id, 'Duplicate enqueue must return existing job');
    assert.equal(job2.deduplicated, true);

    const stats = queue.getStats();
    assert.equal(stats.total, 1, 'Total job count must remain 1');
  });

  await t.test('Step level idempotency: mid-flight crash does not duplicate completed side effects', async () => {
    const db = createDatabase(':memory:');
    const queue = new JobQueue({ db });

    const job = queue.enqueue({
      type: 'payment_order',
      payload: { orderId: 'ORD-100', amount: 50.00 }
    });

    let paymentGatewayCalls = 0;
    let emailReceiptCalls = 0;

    // Simulation of Attempt 1: Crashes right after charging the credit card!
    const leased1 = queue.leaseNextJob('worker-node-1', 5000);
    assert.equal(leased1.id, job.id);
    const stepRunner1 = createStepRunner(queue.db, leased1.id);

    try {
      // Step 1: Charge card
      await stepRunner1('charge_card', async () => {
        paymentGatewayCalls++;
        return { transactionId: 'TX-ABC', amount: 50.00 };
      });

      // Simulated unexpected crash (e.g. out of memory, network cut)
      throw new Error('Process killed mid-flight while preparing shipment!');
    } catch (err) {
      queue.failJob(leased1.id, 'worker-node-1', err);
    }

    assert.equal(paymentGatewayCalls, 1, 'Payment gateway called once in attempt 1');

    // Job was requeued due to backoff
    const intermediateJob = queue.getJobById(job.id);
    assert.equal(intermediateJob.attempts, 1);

    // Fast-forward run_at for test
    db.prepare('UPDATE jobs SET run_at = ? WHERE id = ?').run(Date.now() - 10, job.id);

    // Simulation of Attempt 2: Another worker retries the job
    const leased2 = queue.leaseNextJob('worker-node-2', 5000);
    assert.ok(leased2);
    assert.equal(leased2.id, job.id);
    assert.equal(leased2.attempts, 2);

    let skippedStepLogged = false;
    const stepRunner2 = createStepRunner(queue.db, leased2.id, {
      onStepSkip: (stepName) => {
        if (stepName === 'charge_card') skippedStepLogged = true;
      }
    });

    // Step 1 runs again: MUST NOT invoke paymentGatewayCalls!
    const chargeResult = await stepRunner2('charge_card', async () => {
      paymentGatewayCalls++; // Should never execute!
      return { transactionId: 'TX-WRONG', amount: 50.00 };
    });

    assert.equal(chargeResult.transactionId, 'TX-ABC', 'Reused original transaction ID from DB');
    assert.equal(paymentGatewayCalls, 1, 'Payment gateway must NOT have been called again!');
    assert.equal(skippedStepLogged, true, 'Skipped step hook was called');

    // Step 2: Now completes successfully
    await stepRunner2('send_receipt', async () => {
      emailReceiptCalls++;
      return { sent: true };
    });

    queue.completeJob(leased2.id, 'worker-node-2');

    assert.equal(emailReceiptCalls, 1);
    assert.equal(paymentGatewayCalls, 1, 'Total credit card charges strictly equals 1');

    const finishedJob = queue.getJobById(job.id);
    assert.equal(finishedJob.status, 'completed');
    assert.equal(finishedJob.steps.length, 2);
  });
});
