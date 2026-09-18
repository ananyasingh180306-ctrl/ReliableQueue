import { DatabaseSync } from 'node:sqlite';
import { mkdirSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);
const DEFAULT_DB_PATH = join(__dirname, '..', 'data', 'queue.db');

/**
 * Initializes and configures the SQLite database with WAL mode and schema.
 * @param {string} [dbPath] - Path to SQLite database file.
 * @returns {DatabaseSync} Configured SQLite database instance.
 */
export function createDatabase(dbPath = DEFAULT_DB_PATH) {
  if (dbPath !== ':memory:') {
    mkdirSync(dirname(dbPath), { recursive: true });
  }

  const db = new DatabaseSync(dbPath);

  // Enable WAL mode and performance/safety pragmas for durability and crash-resilience
  db.exec('PRAGMA journal_mode = WAL;');
  db.exec('PRAGMA synchronous = NORMAL;');
  db.exec('PRAGMA foreign_keys = ON;');
  db.exec('PRAGMA busy_timeout = 5000;');

  // Schema creation
  db.exec(`
    CREATE TABLE IF NOT EXISTS jobs (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      type TEXT NOT NULL,
      payload TEXT NOT NULL DEFAULT '{}',
      status TEXT NOT NULL DEFAULT 'queued', -- queued, leased, completed, dead_letter
      priority INTEGER NOT NULL DEFAULT 0,
      run_at INTEGER NOT NULL,
      attempts INTEGER NOT NULL DEFAULT 0,
      max_attempts INTEGER NOT NULL DEFAULT 3,
      backoff_base_ms INTEGER NOT NULL DEFAULT 1000,
      worker_id TEXT DEFAULT NULL,
      leased_at INTEGER DEFAULT NULL,
      lease_expires_at INTEGER DEFAULT NULL,
      last_error TEXT DEFAULT NULL,
      idempotency_key TEXT UNIQUE DEFAULT NULL,
      created_at INTEGER NOT NULL,
      updated_at INTEGER NOT NULL,
      completed_at INTEGER DEFAULT NULL
    );

    CREATE TABLE IF NOT EXISTS job_dependencies (
      parent_job_id INTEGER NOT NULL,
      child_job_id INTEGER NOT NULL,
      PRIMARY KEY (parent_job_id, child_job_id),
      FOREIGN KEY (parent_job_id) REFERENCES jobs(id) ON DELETE CASCADE,
      FOREIGN KEY (child_job_id) REFERENCES jobs(id) ON DELETE CASCADE
    );

    CREATE TABLE IF NOT EXISTS job_steps (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      job_id INTEGER NOT NULL,
      step_name TEXT NOT NULL,
      result TEXT DEFAULT NULL,
      completed_at INTEGER NOT NULL,
      UNIQUE(job_id, step_name),
      FOREIGN KEY (job_id) REFERENCES jobs(id) ON DELETE CASCADE
    );

    -- Index for fast job leasing: status + scheduled execution time + priority
    CREATE INDEX IF NOT EXISTS idx_jobs_eligibility 
      ON jobs(status, run_at, priority DESC);

    -- Index for detecting and recovering expired leases
    CREATE INDEX IF NOT EXISTS idx_jobs_lease_expiry 
      ON jobs(status, lease_expires_at);

    -- Index for quick status statistics
    CREATE INDEX IF NOT EXISTS idx_jobs_status 
      ON jobs(status);
  `);

  return db;
}
