let currentFilter = 'all';
let allJobs = [];

// DOM Elements
const statQueued = document.getElementById('statQueued');
const statLeased = document.getElementById('statLeased');
const statCompleted = document.getElementById('statCompleted');
const statDlq = document.getElementById('statDlq');
const eventFeed = document.getElementById('eventFeed');
const jobsTableBody = document.getElementById('jobsTableBody');
const connectionStatus = document.getElementById('connectionStatus');
const statusText = document.getElementById('statusText');

const jobModal = document.getElementById('jobModal');
const modalCloseBtn = document.getElementById('modalCloseBtn');
const modalJobTitle = document.getElementById('modalJobTitle');
const modalJobContent = document.getElementById('modalJobContent');

// 1. Initialize EventSource (SSE)
function initEventSource() {
  const evtSource = new EventSource('/api/events');

  evtSource.onopen = () => {
    connectionStatus.classList.add('connected');
    statusText.textContent = 'Live SSE Connected';
  };

  evtSource.onerror = () => {
    connectionStatus.classList.remove('connected');
    statusText.textContent = 'Reconnecting...';
  };

  // Metric updates
  evtSource.addEventListener('stats', (e) => {
    const stats = JSON.parse(e.data);
    updateStats(stats);
  });

  // Lifecycle events
  evtSource.addEventListener('job:enqueued', (e) => {
    const job = JSON.parse(e.data);
    addFeedItem('feed-info', `Enqueued job #${job.id} [${job.type}] | Priority: ${job.priority}`);
    fetchJobs();
  });

  evtSource.addEventListener('job:leased', (e) => {
    const data = JSON.parse(e.data);
    const crashTag = data.job.recoveredFromCrash ? ' [CRASH RECOVERED]' : '';
    addFeedItem('feed-leased', `Leased job #${data.job.id} [${data.job.type}] by ${data.workerId} (Attempt ${data.job.attempts})${crashTag}`);
    fetchJobs();
  });

  evtSource.addEventListener('job:step_skipped', (e) => {
    const data = JSON.parse(e.data);
    addFeedItem('feed-idempotency', `⚡ [Idempotency] Step '${data.stepName}' on job #${data.jobId} skipped! Reused previous result.`);
    fetchJobs();
  });

  evtSource.addEventListener('job:step_completed', (e) => {
    const data = JSON.parse(e.data);
    addFeedItem('feed-info', `✓ Step '${data.stepName}' on job #${data.jobId} completed.`);
  });

  evtSource.addEventListener('job:completed', (e) => {
    const data = JSON.parse(e.data);
    addFeedItem('feed-completed', `✓ Job #${data.job.id} [${data.job.type}] finished successfully.`);
    fetchJobs();
  });

  evtSource.addEventListener('job:failed', (e) => {
    const data = JSON.parse(e.data);
    if (data.status === 'dead_letter') {
      addFeedItem('feed-dlq', `✖ Job #${data.job.id} moved to DEAD-LETTER QUEUE after ${data.job.attempts} attempts! Error: ${data.error}`);
    } else {
      const sec = (data.retryInMs / 1000).toFixed(1);
      addFeedItem('feed-failed', `⚠ Job #${data.job.id} failed: ${data.error}. Retrying in ${sec}s.`);
    }
    fetchJobs();
  });
}

function updateStats(stats) {
  statQueued.textContent = stats.queued ?? 0;
  statLeased.textContent = stats.leased ?? 0;
  statCompleted.textContent = stats.completed ?? 0;
  statDlq.textContent = stats.dead_letter ?? 0;
}

function addFeedItem(typeClass, message) {
  const item = document.createElement('div');
  item.className = `feed-item ${typeClass}`;
  const time = new Date().toLocaleTimeString();
  item.innerHTML = `
    <span class="feed-time">${time}</span>
    <span class="feed-msg">${message}</span>
  `;
  eventFeed.prepend(item);

  // Keep feed max 40 items
  while (eventFeed.children.length > 40) {
    eventFeed.removeChild(eventFeed.lastChild);
  }
}

// 2. Fetch and render jobs
async function fetchJobs() {
  try {
    const res = await fetch('/api/jobs');
    allJobs = await res.json();
    renderJobs();
  } catch (err) {
    console.error('Failed to fetch jobs:', err);
  }
}

function renderJobs() {
  const filtered = currentFilter === 'all' 
    ? allJobs 
    : allJobs.filter(j => j.status === currentFilter);

  if (filtered.length === 0) {
    jobsTableBody.innerHTML = `
      <tr>
        <td colspan="7" class="empty-state">No jobs found matching "${currentFilter}"</td>
      </tr>
    `;
    return;
  }

  jobsTableBody.innerHTML = filtered.map(job => {
    const scheduledTime = new Date(job.runAt).toLocaleTimeString();
    const canReplay = job.status === 'dead_letter';

    return `
      <tr>
        <td><strong>#${job.id}</strong></td>
        <td><code>${job.type}</code></td>
        <td><span class="status-badge status-${job.status}">${job.status}</span></td>
        <td>${job.priority}</td>
        <td>${job.attempts} / ${job.maxAttempts}</td>
        <td>${scheduledTime}</td>
        <td>
          <button class="btn btn-xs btn-outline" onclick="inspectJob(${job.id})">Inspect</button>
          ${canReplay ? `<button class="btn btn-xs btn-replay-all" onclick="replayJob(${job.id})">Replay</button>` : ''}
        </td>
      </tr>
    `;
  }).join('');
}

// Filter pills
document.querySelectorAll('.filter-pills .pill').forEach(pill => {
  pill.addEventListener('click', () => {
    document.querySelectorAll('.filter-pills .pill').forEach(p => p.classList.remove('active'));
    pill.classList.add('active');
    currentFilter = pill.getAttribute('data-filter');
    renderJobs();
  });
});

// Modal inspection
window.inspectJob = async function(id) {
  try {
    const res = await fetch(`/api/jobs/${id}`);
    const job = await res.json();

    modalJobTitle.textContent = `Job #${job.id} [${job.type}]`;
    modalJobContent.innerHTML = `
      <div class="detail-row">
        <div class="detail-label">Status & Attempts</div>
        <div>
          <span class="status-badge status-${job.status}">${job.status}</span>
          <span>&nbsp; Attempt ${job.attempts} of ${job.maxAttempts}</span>
        </div>
      </div>

      <div class="detail-row">
        <div class="detail-label">Payload</div>
        <pre style="background:#0f172a; padding:10px; border-radius:6px; font-family:var(--font-mono); overflow-x:auto;">${JSON.stringify(job.payload, null, 2)}</pre>
      </div>

      ${job.lastError ? `
        <div class="detail-row">
          <div class="detail-label" style="color:var(--danger)">Failure Error / Stack Trace</div>
          <pre style="background:#2a1115; color:#fca5a5; padding:10px; border-radius:6px; font-family:var(--font-mono); font-size:11px; overflow-x:auto;">${job.lastError}</pre>
        </div>
      ` : ''}

      ${job.parentDependencies?.length > 0 ? `
        <div class="detail-row">
          <div class="detail-label">Parent Dependencies</div>
          <div>${job.parentDependencies.map(d => `<span class="status-badge status-${d.status}">#${d.id} [${d.type}] - ${d.status}</span>`).join(' ')}</div>
        </div>
      ` : ''}

      <div class="detail-row">
        <div class="detail-label">Executed Idempotency Steps (${job.steps?.length || 0})</div>
        ${job.steps && job.steps.length > 0 ? job.steps.map(s => `
          <div class="step-card">
            <strong>✓ ${s.step_name}</strong>
            <div style="font-size:11px; color:var(--text-muted);">Recorded at: ${new Date(s.completed_at).toLocaleTimeString()}</div>
            ${s.result ? `<div style="font-size:11px; margin-top:4px;">Result: <code>${JSON.stringify(s.result)}</code></div>` : ''}
          </div>
        `).join('') : '<div style="color:var(--text-muted)">No steps executed yet.</div>'}
      </div>

      ${job.status === 'dead_letter' ? `
        <div style="margin-top:20px;">
          <button class="btn btn-warning" style="width:100%;" onclick="replayJob(${job.id}); closeModal();">Replay Dead-Letter Job</button>
        </div>
      ` : ''}
    `;

    jobModal.classList.add('open');
  } catch (err) {
    alert('Error fetching job details: ' + err.message);
  }
};

window.closeModal = function() {
  jobModal.classList.remove('open');
};

modalCloseBtn.addEventListener('click', closeModal);
jobModal.addEventListener('click', (e) => {
  if (e.target === jobModal) closeModal();
});

// Replay action
window.replayJob = async function(id) {
  try {
    const res = await fetch(`/api/dlq/${id}/replay`, { method: 'POST' });
    const data = await res.json();
    if (data.success) {
      addFeedItem('feed-info', `Replayed DLQ job #${id} back to queued!`);
      fetchJobs();
    }
  } catch (err) {
    alert('Replay failed: ' + err.message);
  }
};

// Replay all action
document.getElementById('btnReplayAll').addEventListener('click', async () => {
  try {
    const res = await fetch('/api/dlq/replay-all', { method: 'POST' });
    const data = await res.json();
    if (data.success) {
      addFeedItem('feed-info', `Replayed ${data.replayedCount} dead-letter jobs back to queue!`);
      fetchJobs();
    }
  } catch (err) {
    alert('Replay all failed: ' + err.message);
  }
});

// Demo Buttons
document.getElementById('btnDemoCrash').addEventListener('click', async () => {
  addFeedItem('feed-info', 'Simulating: 3-step payment job with worker crash on step 2...');
  await fetch('/api/demo/simulate-crash', { method: 'POST' });
});

document.getElementById('btnDemoDlq').addEventListener('click', async () => {
  addFeedItem('feed-info', 'Simulating: Failing task that retries with exponential backoff...');
  await fetch('/api/demo/simulate-dlq', { method: 'POST' });
});

document.getElementById('btnDemoDag').addEventListener('click', async () => {
  addFeedItem('feed-info', 'Simulating: Parent Job A & Dependent Child Job B...');
  await fetch('/api/demo/simulate-dag', { method: 'POST' });
});

document.getElementById('btnDemoPriority').addEventListener('click', async () => {
  addFeedItem('feed-info', 'Enqueuing: Normal priority job vs High priority job...');
  await fetch('/api/jobs', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ type: 'email', payload: { to: 'delayed@example.com' }, priority: 1, delayMs: 4000 })
  });
  await fetch('/api/jobs', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ type: 'email', payload: { to: 'urgent@example.com' }, priority: 10, delayMs: 0 })
  });
});

document.getElementById('refreshBtn').addEventListener('click', () => {
  fetchJobs();
  fetch('/api/stats').then(r => r.json()).then(updateStats);
});

// Boot
initEventSource();
fetchJobs();
fetch('/api/stats').then(r => r.json()).then(updateStats);
