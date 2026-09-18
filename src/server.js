import http from 'node:http';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { WorkerPool } from './worker.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const PUBLIC_DIR = path.join(__dirname, '..', 'public');

/**
 * Starts the HTTP Web Dashboard and REST API server.
 * @param {import('./queue.js').JobQueue} queue
 * @param {number} [port=3000]
 * @param {object} [options]
 * @param {boolean} [options.embeddedWorker=true]
 */
export function startServer(queue, port = 3000, options = {}) {
  const clients = new Set();

  function broadcast(eventType, data) {
    const message = `event: ${eventType}\ndata: ${JSON.stringify(data)}\n\n`;
    for (const client of clients) {
      try {
        client.write(message);
      } catch {
        clients.delete(client);
      }
    }
  }

  // Optionally launch embedded background worker pool
  let workerPool = null;
  if (options.embeddedWorker !== false) {
    workerPool = new WorkerPool(queue, { concurrency: 2, visibilityTimeoutMs: 15000 });
    workerPool.on('job:leased', (evt) => broadcast('job:leased', evt));
    workerPool.on('job:completed', (evt) => {
      broadcast('job:completed', evt);
      broadcast('stats', queue.getStats());
    });
    workerPool.on('job:failed', (evt) => {
      broadcast('job:failed', evt);
      broadcast('stats', queue.getStats());
    });
    workerPool.on('job:step_skipped', (evt) => broadcast('job:step_skipped', evt));
    workerPool.on('job:step_completed', (evt) => broadcast('job:step_completed', evt));
    workerPool.on('job:log', (evt) => broadcast('job:log', evt));
    workerPool.start();
  }

  const server = http.createServer((req, res) => {
    const url = new URL(req.url, `http://${req.headers.host}`);

    // CORS headers for flexibility
    res.setHeader('Access-Control-Allow-Origin', '*');
    res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
    res.setHeader('Access-Control-Allow-Headers', 'Content-Type');

    if (req.method === 'OPTIONS') {
      res.writeHead(204);
      res.end();
      return;
    }

    // Helper for JSON responses
    const json = (data, status = 200) => {
      res.writeHead(status, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify(data));
    };

    // Helper to read request body
    const readBody = () => {
      return new Promise((resolve, reject) => {
        let body = '';
        req.on('data', chunk => { body += chunk; });
        req.on('end', () => {
          try {
            resolve(body ? JSON.parse(body) : {});
          } catch (e) {
            reject(new Error('Invalid JSON payload'));
          }
        });
        req.on('error', reject);
      });
    };

    // SSE Stream for real-time dashboard
    if (url.pathname === '/api/events') {
      res.writeHead(200, {
        'Content-Type': 'text/event-stream',
        'Cache-Control': 'no-cache',
        'Connection': 'keep-alive'
      });
      res.write(': connected\n\n');
      clients.add(res);

      // Send initial snapshot
      res.write(`event: stats\ndata: ${JSON.stringify(queue.getStats())}\n\n`);

      req.on('close', () => {
        clients.delete(res);
      });
      return;
    }

    // REST API endpoints
    if (url.pathname === '/api/stats' && req.method === 'GET') {
      return json(queue.getStats());
    }

    if (url.pathname === '/api/jobs' && req.method === 'GET') {
      return json(queue.getRecentJobs(50));
    }

    if (url.pathname.startsWith('/api/jobs/') && req.method === 'GET') {
      const id = parseInt(url.pathname.split('/')[3], 10);
      const job = queue.getJobById(id);
      return job ? json(job) : json({ error: 'Job not found' }, 404);
    }

    if (url.pathname === '/api/inflight' && req.method === 'GET') {
      return json(queue.getInFlightJobs());
    }

    if (url.pathname === '/api/dlq' && req.method === 'GET') {
      return json(queue.getDeadLetterJobs());
    }

    if (url.pathname === '/api/jobs' && req.method === 'POST') {
      readBody()
        .then(body => {
          const job = queue.enqueue(body);
          broadcast('job:enqueued', job);
          broadcast('stats', queue.getStats());
          json(job, 201);
        })
        .catch(err => json({ error: err.message }, 400));
      return;
    }

    if (url.pathname.startsWith('/api/dlq/') && url.pathname.endsWith('/replay') && req.method === 'POST') {
      const id = parseInt(url.pathname.split('/')[3], 10);
      const ok = queue.replayDeadLetterJob(id);
      if (ok) {
        broadcast('stats', queue.getStats());
        return json({ success: true, replayedJobId: id });
      } else {
        return json({ error: 'Job not found in DLQ' }, 404);
      }
    }

    if (url.pathname === '/api/dlq/replay-all' && req.method === 'POST') {
      const count = queue.replayAllDeadLetterJobs();
      broadcast('stats', queue.getStats());
      return json({ success: true, replayedCount: count });
    }

    // Interactive Demo Trigger Endpoints
    if (url.pathname === '/api/demo/simulate-crash' && req.method === 'POST') {
      // Demonstrates Idempotency & Crash recovery:
      // Job executes Step 1 (charges card), then simulates crash on Step 2.
      // Retried job detects Step 1 was already run and DOES NOT double-charge!
      const job = queue.enqueue({
        type: 'payment_order',
        payload: {
          orderId: `ORD-${Math.floor(1000 + Math.random() * 9000)}`,
          amount: (Math.random() * 200 + 20).toFixed(2),
          simulateCrash: true
        },
        maxAttempts: 3,
        backoffBaseMs: 1500
      });
      broadcast('job:enqueued', job);
      broadcast('stats', queue.getStats());
      return json({ message: 'Crash simulation job enqueued', job });
    }

    if (url.pathname === '/api/demo/simulate-dlq' && req.method === 'POST') {
      // Demonstrates Exponential Backoff & DLQ transition
      const job = queue.enqueue({
        type: 'failing_task',
        payload: { errorMessage: 'Simulated persistent database outage' },
        maxAttempts: 3,
        backoffBaseMs: 1000
      });
      broadcast('job:enqueued', job);
      broadcast('stats', queue.getStats());
      return json({ message: 'Failing job enqueued for DLQ demo', job });
    }

    if (url.pathname === '/api/demo/simulate-dag' && req.method === 'POST') {
      // Demonstrates Job Dependency graph (Stretch Goal 1):
      // Parent Job A must finish before Child Job B runs.
      const parent = queue.enqueue({
        type: 'email',
        payload: { to: 'customer@test.com', subject: 'Step 1: Welcome Parent Job' }
      });
      const child = queue.enqueue({
        type: 'email',
        payload: { to: 'manager@test.com', subject: 'Step 2: Manager Notification Child Job' },
        dependsOn: [parent.id]
      });
      broadcast('job:enqueued', parent);
      broadcast('job:enqueued', child);
      broadcast('stats', queue.getStats());
      return json({ message: 'DAG dependent jobs enqueued', parent, child });
    }

    // Static files serving
    let filePath = path.join(PUBLIC_DIR, url.pathname === '/' ? 'index.html' : url.pathname);
    if (!fs.existsSync(filePath) || fs.statSync(filePath).isDirectory()) {
      filePath = path.join(PUBLIC_DIR, 'index.html');
    }

    const ext = path.extname(filePath).toLowerCase();
    const mimeTypes = {
      '.html': 'text/html',
      '.css': 'text/css',
      '.js': 'application/javascript',
      '.json': 'application/json',
      '.png': 'image/png',
      '.svg': 'image/svg+xml'
    };

    fs.readFile(filePath, (err, content) => {
      if (err) {
        res.writeHead(404, { 'Content-Type': 'text/plain' });
        res.end('Not Found');
      } else {
        res.writeHead(200, { 'Content-Type': mimeTypes[ext] || 'text/plain' });
        res.end(content);
      }
    });
  });

  server.listen(port, '0.0.0.0', () => {
    console.log(`\n======================================================`);
    console.log(`🚀 Web Dashboard & REST API live at: http://0.0.0.0:${port}`);
    console.log(`======================================================\n`);
  });

  return { server, workerPool };
}

// Start standalone if executed directly
if (process.argv[1] === fileURLToPath(import.meta.url)) {
  import('./queue.js').then(({ JobQueue }) => {
    const queue = new JobQueue();
    const port = process.env.PORT ? parseInt(process.env.PORT, 10) : 3000;
    startServer(queue, port);
  });
}
