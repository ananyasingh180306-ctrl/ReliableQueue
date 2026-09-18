import { setTimeout as sleep } from 'node:timers/promises';

/**
 * Task registry containing standard and demonstrative job handlers.
 */
export const taskRegistry = {
  /**
   * Simple asynchronous email task.
   */
  async email(job, { log }) {
    log(`Sending email to ${job.payload.to || 'user@example.com'} (subject: "${job.payload.subject || 'Hello'}")`);
    await sleep(400);
    return { sent: true, recipient: job.payload.to, timestamp: Date.now() };
  },

  /**
   * Task designed to fail to demonstrate retries, exponential backoff, and DLQ.
   */
  async failing_task(job, { log }) {
    log(`Executing failing_task (attempt ${job.attempts}/${job.maxAttempts})`);
    await sleep(200);
    if (job.payload?.succeedOnAttempt && job.attempts >= job.payload.succeedOnAttempt) {
      log(`failing_task successfully healed on attempt ${job.attempts}!`);
      return { healed: true, attempt: job.attempts };
    }
    throw new Error(job.payload?.errorMessage || `Simulated transient failure on attempt ${job.attempts}`);
  },

  /**
   * Multi-step order fulfillment task demonstrating Core Requirement 07 (Idempotency).
   * Even if a worker crashes after step 1, the step ledger prevents double-charging!
   */
  async payment_order(job, { step, log }) {
    const { orderId = 'ORD-9901', amount = 149.99, simulateCrash = false } = job.payload;
    log(`Starting order fulfillment for ${orderId} (\$${amount})`);

    // Step 1: Charge customer credit card
    const payment = await step('charge_credit_card', async () => {
      log(`[STEP 1] Charging payment gateway for ${orderId}: \$${amount}`);
      await sleep(300);
      return {
        transactionId: `txn_${Date.now()}_${Math.random().toString(36).slice(2, 7)}`,
        amount,
        chargedAt: Date.now()
      };
    });

    log(`Payment confirmed: ${payment.transactionId}`);

    // Step 2: Reserve warehouse inventory (can trigger crash simulation)
    const inventory = await step('reserve_inventory', async () => {
      if (simulateCrash && job.attempts === 1) {
        log(`[CRASH SIMULATION] Worker crashing mid-task during warehouse allocation!`);
        throw new Error('Mid-task crash simulation: process killed after payment was already charged');
      }
      log(`[STEP 2] Reserving warehouse inventory for ${orderId}`);
      await sleep(300);
      return { warehouse: 'WH-East', reserved: true, sku: 'PROD-404' };
    });

    // Step 3: Dispatch confirmation receipt
    const receipt = await step('send_receipt', async () => {
      log(`[STEP 3] Dispatching receipt email to customer for ${orderId}`);
      await sleep(200);
      return { emailed: true, orderId };
    });

    log(`Order ${orderId} successfully completed without double-charging!`);
    return {
      orderId,
      payment,
      inventory,
      receipt,
      totalAttempts: job.attempts
    };
  },

  /**
   * Long-running task to demonstrate visibility timeout heartbeats.
   */
  async long_running(job, { heartbeat, log }) {
    const durationSeconds = job.payload?.seconds || 5;
    log(`Starting long running task for ${durationSeconds} seconds...`);
    for (let i = 1; i <= durationSeconds; i++) {
      await sleep(1000);
      heartbeat();
      log(`Tick ${i}/${durationSeconds} (lease extended via heartbeat)`);
    }
    return { completed: true, elapsedSeconds: durationSeconds };
  }
};
