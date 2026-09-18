/**
 * Provides step-level idempotency to prevent double-execution of side effects
 * when a job crashes mid-flight and is retried.
 */

/**
 * Creates a step runner scoped to a specific job ID.
 * @param {import('node:sqlite').DatabaseSync} db - SQLite database instance.
 * @param {number} jobId - ID of the currently executing job.
 * @param {object} [options]
 * @param {function} [options.onStepSkip] - Callback when an existing step is skipped.
 * @param {function} [options.onStepComplete] - Callback when a new step completes.
 * @returns {function(string, Function): Promise<any>}
 */
export function createStepRunner(db, jobId, options = {}) {
  const getStepStmt = db.prepare(`
    SELECT result FROM job_steps WHERE job_id = ? AND step_name = ?;
  `);

  const insertStepStmt = db.prepare(`
    INSERT INTO job_steps (job_id, step_name, result, completed_at)
    VALUES (?, ?, ?, ?);
  `);

  return async function step(stepName, actionFn) {
    // 1. Check if step was already completed in a prior attempt
    const existing = getStepStmt.get(jobId, stepName);
    if (existing) {
      const parsedResult = existing.result ? JSON.parse(existing.result) : null;
      if (options.onStepSkip) {
        options.onStepSkip(stepName, parsedResult);
      }
      return parsedResult;
    }

    // 2. Execute the action since it has not been done yet
    const result = await actionFn();

    // 3. Persist the step outcome atomically
    const serialized = result !== undefined ? JSON.stringify(result) : null;
    insertStepStmt.run(jobId, stepName, serialized, Date.now());

    if (options.onStepComplete) {
      options.onStepComplete(stepName, result);
    }

    return result;
  };
}

/**
 * Retrieves all completed steps for a job.
 * @param {import('node:sqlite').DatabaseSync} db
 * @param {number} jobId
 * @returns {Array<{step_name: string, result: any, completed_at: number}>}
 */
export function getJobSteps(db, jobId) {
  const stmt = db.prepare(`
    SELECT step_name, result, completed_at 
    FROM job_steps 
    WHERE job_id = ? 
    ORDER BY id ASC;
  `);
  const rows = stmt.all(jobId);
  return rows.map(r => ({
    step_name: r.step_name,
    result: r.result ? JSON.parse(r.result) : null,
    completed_at: r.completed_at
  }));
}
