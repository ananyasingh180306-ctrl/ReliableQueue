#!/usr/bin/env node
import { JobQueue } from './queue.js';
import { WorkerPool } from './worker.js';
import { startServer } from './server.js';

const queue = new JobQueue();

// Formatting helpers
const c = {
  reset: '\x1b[0m',
  bold: '\x1b[1m',
  green: '\x1b[32m',
  yellow: '\x1b[33m',
  red: '\x1b[31m',
  cyan: '\x1b[36m',
  gray: '\x1b[90m',
  magenta: '\x1b[35m'
};

function printBanner() {
  console.log(`${c.bold}${c.cyan}====================================================${c.reset}`);
  console.log(`${c.bold}${c.cyan}      RELIABLE JOB QUEUE ENGINE (Node.js/WAL)       ${c.reset}`);
  console.log(`${c.bold}${c.cyan}====================================================${c.reset}\n`);
}

function parseArgs(args) {
  const flags = {};
  const positional = [];
  for (let i = 0; i < args.length; i++) {
    const arg = args[i];
    if (arg.startsWith('--')) {
      const key = arg.slice(2);
      const next = args[i + 1];
      if (next && !next.startsWith('--')) {
        flags[key] = next;
        i++;
      } else {
        flags[key] = true;
      }
    } else {
      positional.push(arg);
    }
  }
  return { flags, positional };
}

async function main() {
  const [, , command, ...rawArgs] = process.argv;
  const { flags, positional } = parseArgs(rawArgs);

  if (!command || command === 'help') {
    printBanner();
    console.log(`${c.bold}Usage:${c.reset}`);
    console.log(`  node src/cli.js <command> [options]\n`);
    console.log(`${c.bold}Commands:${c.reset}`);
    console.log(`  ${c.green}enqueue <type> [jsonPayload]${c.reset}  Enqueue a new job`);
    console.log(`     --priority <N>               Set job priority (higher runs first)`);
    console.log(`     --delay <seconds>            Delay job execution`);
    console.log(`     --idempotency-key <key>      Prevent duplicate submissions`);
    console.log(`     --max-attempts <N>           Max retry attempts before DLQ`);
    console.log(`     --depends-on <id1,id2>       Wait for parent jobs to complete\n`);
    console.log(`  ${c.green}status${c.reset}                        Inspect queue depth & metrics`);
    console.log(`  ${c.green}inflight${c.reset}                      List currently leased in-flight jobs`);
    console.log(`  ${c.green}inspect <jobId>${c.reset}               Inspect a job and its completed steps`);
    console.log(`  ${c.green}dlq list${c.reset}                      View dead-letter queue contents & errors`);
    console.log(`  ${c.green}dlq replay <id|all>${c.reset}          Replay dead-lettered job(s)`);
    console.log(`  ${c.green}worker${c.reset}                        Run background worker process`);
    console.log(`     --concurrency <N>            Number of concurrent slots (default: 2)`);
    console.log(`     --visibility <seconds>       Lease timeout in seconds (default: 15)`);
    console.log(`  ${c.green}server${c.reset}                        Start Web UI and REST API server`);
    console.log(`     --port <N>                   Port number (default: 3000)\n`);
    return;
  }

  switch (command) {
    case 'status':
    case 'stats': {
      const stats = queue.getStats();
      console.log(`\n${c.bold}Queue Health Overview:${c.reset}`);
      console.log(`  ${c.yellow}Queued:${c.reset}      ${stats.queued}`);
      console.log(`  ${c.cyan}In-Flight:${c.reset}   ${stats.leased}`);
      console.log(`  ${c.green}Completed:${c.reset}   ${stats.completed}`);
      console.log(`  ${c.red}Dead-Letter:${c.reset} ${stats.dead_letter}`);
      console.log(`  ${c.bold}Total Jobs:${c.reset}  ${stats.total}\n`);
      break;
    }

    case 'enqueue': {
      const type = positional[0];
      if (!type) {
        console.error(`${c.red}Error: Job type is required. (e.g. node src/cli.js enqueue email '{"to":"foo@bar.com"}') ${c.reset}`);
        process.exit(1);
      }
function parsePayload(raw) {
  if (!raw) return {};
  if (typeof raw === 'object') return raw;
  const str = String(raw).trim();
  if (!str) return {};

  try {
    return JSON.parse(str);
  } catch {}

  try {
    const fixed = str.replace(/'/g, '"');
    return JSON.parse(fixed);
  } catch {}

  try {
    if (str.startsWith('{') && str.endsWith('}')) {
      const inner = str.slice(1, -1).trim();
      const obj = {};
      const pairs = inner.split(/,(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)/);
      for (const pair of pairs) {
        const [k, ...vParts] = pair.split(':');
        if (k) {
          const key = k.trim().replace(/^["']|["']$/g, '');
          const val = vParts.join(':').trim().replace(/^["']|["']$/g, '');
          obj[key] = val;
        }
      }
      if (Object.keys(obj).length > 0) return obj;
    }
  } catch {}

  if (str.includes('=')) {
    const obj = {};
    const tokens = str.split(/\s+/);
    for (const t of tokens) {
      const [k, ...vParts] = t.split('=');
      if (k && vParts.length > 0) {
        obj[k.trim()] = vParts.join('=').trim().replace(/^["']|["']$/g, '');
      }
    }
    if (Object.keys(obj).length > 0) return obj;
  }

  return { data: str };
}

      const payload = parsePayload(positional[1]);

      const priority = flags.priority ? parseInt(flags.priority, 10) : 0;
      const delayMs = flags.delay ? parseInt(flags.delay, 10) * 1000 : 0;
      const idempotencyKey = flags['idempotency-key'] || null;
      const maxAttempts = flags['max-attempts'] ? parseInt(flags['max-attempts'], 10) : 3;
      const dependsOn = flags['depends-on'] ? flags['depends-on'].split(',').map(Number) : [];

      const job = queue.enqueue({
        type,
        payload,
        priority,
        delayMs,
        idempotencyKey,
        maxAttempts,
        dependsOn
      });

      if (job.deduplicated) {
        console.log(`${c.yellow}[Deduplicated] Job with idempotency key "${idempotencyKey}" already exists! Job ID: #${job.id}${c.reset}`);
      } else {
        console.log(`${c.green}✓ Enqueued job #${job.id} [${job.type}] | Priority: ${job.priority} | Scheduled: ${new Date(job.runAt).toLocaleTimeString()}${c.reset}`);
        if (dependsOn.length > 0) {
          console.log(`  Waiting on parent job(s): ${dependsOn.join(', ')}`);
        }
      }
      break;
    }

    case 'inflight': {
      const jobs = queue.getInFlightJobs();
      if (jobs.length === 0) {
        console.log(`${c.gray}No in-flight jobs currently executing.${c.reset}`);
        return;
      }
      console.log(`\n${c.bold}Active In-Flight Jobs (${jobs.length}):${c.reset}`);
      for (const j of jobs) {
        const remainingSec = (j.remainingLeaseMs / 1000).toFixed(1);
        console.log(`  Job #${j.id} [${j.type}] | Worker: ${c.cyan}${j.workerId}${c.reset} | Attempt: ${j.attempts}/${j.maxAttempts} | Lease expires in: ${c.yellow}${remainingSec}s${c.reset}`);
      }
      console.log();
      break;
    }

    case 'inspect': {
      const jobId = parseInt(positional[0], 10);
      if (isNaN(jobId)) {
        console.error(`${c.red}Please specify a numeric jobId.${c.reset}`);
        process.exit(1);
      }
      const job = queue.getJobById(jobId);
      if (!job) {
        console.error(`${c.red}Job #${jobId} not found.${c.reset}`);
        process.exit(1);
      }

      console.log(`\n${c.bold}Job #${job.id} [${job.type}]${c.reset}`);
      console.log(`  Status:       ${job.status}`);
      console.log(`  Priority:     ${job.priority}`);
      console.log(`  Attempts:     ${job.attempts} / ${job.maxAttempts}`);
      console.log(`  Payload:      ${JSON.stringify(job.payload)}`);
      if (job.lastError) {
        console.log(`  ${c.red}Last Error:   ${job.lastError}${c.reset}`);
      }
      if (job.parentDependencies?.length > 0) {
        console.log(`  Dependencies: ${job.parentDependencies.map(d => `#${d.id} (${d.status})`).join(', ')}`);
      }
      if (job.steps?.length > 0) {
        console.log(`  ${c.bold}Completed Steps (${job.steps.length}):${c.reset}`);
        for (const s of job.steps) {
          console.log(`    - ${c.green}✓ ${s.step_name}${c.reset} (recorded at ${new Date(s.completed_at).toLocaleTimeString()})`);
          if (s.result) console.log(`      Result: ${JSON.stringify(s.result)}`);
        }
      }
      console.log();
      break;
    }

    case 'dlq': {
      const sub = positional[0];
      if (sub === 'list') {
        const dlq = queue.getDeadLetterJobs();
        if (dlq.length === 0) {
          console.log(`${c.green}Dead-letter queue is clean. No failed jobs.${c.reset}`);
          return;
        }
        console.log(`\n${c.bold}${c.red}Dead-Letter Queue (${dlq.length} jobs):${c.reset}`);
        for (const j of dlq) {
          console.log(`  ${c.red}✖ Job #${j.id} [${j.type}]${c.reset} | Failed after ${j.attempts} attempts`);
          console.log(`    ${c.gray}Reason: ${j.lastError?.split('\n')[0] || 'Unknown'}${c.reset}`);
        }
        console.log(`\nTo replay a job: node src/cli.js dlq replay <id> (or 'all')\n`);
      } else if (sub === 'replay') {
        const target = positional[1];
        if (!target) {
          console.error(`${c.red}Usage: node src/cli.js dlq replay <jobId|all>${c.reset}`);
          process.exit(1);
        }
        if (target.toLowerCase() === 'all') {
          const count = queue.replayAllDeadLetterJobs();
          console.log(`${c.green}✓ Replayed ${count} dead-letter jobs back to 'queued' status.${c.reset}`);
        } else {
          const id = parseInt(target, 10);
          const ok = queue.replayDeadLetterJob(id);
          if (ok) {
            console.log(`${c.green}✓ Replayed dead-letter job #${id} back into the queue.${c.reset}`);
          } else {
            console.error(`${c.red}Job #${id} is not in the dead-letter queue.${c.reset}`);
          }
        }
      } else {
        console.log(`Unknown dlq subcommand. Use 'list' or 'replay <id|all>'`);
      }
      break;
    }

    case 'worker': {
      const concurrency = flags.concurrency ? parseInt(flags.concurrency, 10) : 2;
      const visibilitySec = flags.visibility ? parseInt(flags.visibility, 10) : 15;
      const pool = new WorkerPool(queue, {
        concurrency,
        visibilityTimeoutMs: visibilitySec * 1000
      });

      printBanner();
      console.log(`${c.green}Starting worker pool [${pool.workerId}] with ${concurrency} concurrency slot(s)...${c.reset}`);
      console.log(`${c.gray}Visibility timeout: ${visibilitySec}s. Press Ctrl+C to stop.\n${c.reset}`);

      pool.on('job:leased', ({ slotIndex, job }) => {
        const recoveryTag = job.recoveredFromCrash ? ` ${c.magenta}[CRASH RECOVERED]${c.reset}` : '';
        console.log(`[Slot ${slotIndex}] ${c.cyan}Leased job #${job.id} [${job.type}]${c.reset} (Attempt ${job.attempts}/${job.maxAttempts})${recoveryTag}`);
      });

      pool.on('job:completed', ({ job, result }) => {
        console.log(`${c.green}✓ [Job #${job.id}] Completed successfully!${c.reset}`);
      });

      pool.on('job:failed', ({ job, error, status, retryInMs }) => {
        if (status === 'dead_letter') {
          console.log(`${c.red}✖ [Job #${job.id}] Exceeded max retries (${job.attempts}/${job.maxAttempts}). Moved to DEAD-LETTER QUEUE!${c.reset}`);
        } else {
          const delaySec = (retryInMs / 1000).toFixed(1);
          console.log(`${c.yellow}⚠ [Job #${job.id}] Failed: ${error}. Retrying in ${delaySec}s (Exponential backoff)...${c.reset}`);
        }
      });

      pool.on('job:step_skipped', ({ jobId, stepName }) => {
        console.log(`  ${c.magenta}[Idempotency] Step '${stepName}' on job #${jobId} already completed. Skipped duplicate side-effect!${c.reset}`);
      });

      pool.on('job:log', ({ jobId, message }) => {
        console.log(`  [Job #${jobId}] ${message}`);
      });

      pool.start();

      process.on('SIGINT', async () => {
        console.log(`\n${c.yellow}Gracefully shutting down worker pool...${c.reset}`);
        await pool.stop();
        console.log(`${c.green}Worker pool stopped safely.${c.reset}`);
        process.exit(0);
      });
      break;
    }

    case 'server': {
      const port = flags.port ? parseInt(flags.port, 10) : 3000;
      startServer(queue, port);
      break;
    }

    default:
      console.error(`${c.red}Unknown command: ${command}. Run 'node src/cli.js help' for usage.${c.reset}`);
      process.exit(1);
  }
}

main().catch(err => {
  console.error(`${c.red}Fatal Error:${c.reset}`, err);
  process.exit(1);
});
