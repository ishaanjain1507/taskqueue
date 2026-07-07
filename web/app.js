document.addEventListener('DOMContentLoaded', () => {
    const jobForm = document.getElementById('jobForm');
    const singleBtn = document.getElementById('singleBtn');
    const simulateBtn = document.getElementById('simulateBtn');
    const scaleBtn = document.getElementById('scaleBtn');
    
    const feedContainer = document.getElementById('feedContainer');
    const toastContainer = document.getElementById('toastContainer');
    const recentJobsBody = document.getElementById('recentJobsBody');
    const dlqJobsBody = document.getElementById('dlqJobsBody');

    // Tabs logic
    document.querySelectorAll('.tab-btn').forEach(btn => {
        btn.addEventListener('click', (e) => {
            document.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));
            document.querySelectorAll('.tab-content').forEach(c => c.classList.remove('active'));
            e.target.classList.add('active');
            document.getElementById(e.target.dataset.target).classList.add('active');
        });
    });

    const statElements = {
        pending: document.getElementById('stat-pending'),
        processing: document.getElementById('stat-processing'),
        success: document.getElementById('stat-success'),
        failed: document.getElementById('stat-failed')
    };

    let hasActivity = false;

    async function fetchStats() {
        try {
            const res = await fetch('/stats');
            if (!res.ok) return;
            const data = await res.json();
            
            const hist = data.historical || {};
            const pending = hist['PENDING'] || 0;
            const processing = hist['PROCESSING'] || 0;
            const success = hist['SUCCESS'] || 0;
            const failed = (hist['FAILED'] || 0) + (hist['DEAD'] || 0);

            animateValue(statElements.pending, pending);
            animateValue(statElements.processing, processing);
            animateValue(statElements.success, success);
            animateValue(statElements.failed, failed);
            
            const aw = document.getElementById('activeWorkersVal');
            if (aw) {
                animateValue(aw, data.active_workers || 0);
            }
        } catch (error) {
            console.error('Stats fetch error:', error);
        }
    }

    function formatDuration(ms) {
        if (ms < 0) return "-";
        if (ms < 1000) return ms + "ms";
        return (ms / 1000).toFixed(2) + "s";
    }

    async function fetchRecentJobs() {
        try {
            const res = await fetch('/jobs/recent');
            if (!res.ok) return;
            const jobs = await res.json();
            
            recentJobsBody.innerHTML = '';

            if (jobs && jobs.length > 0) {
                jobs.forEach(job => {
                    const shortId = job.id.split('-')[0];
                    
                    const created = new Date(job.created_at).getTime();
                    const started = job.started_at ? new Date(job.started_at).getTime() : null;
                    const completed = job.completed_at ? new Date(job.completed_at).getTime() : null;
                    const now = Date.now();

                    // Calculate metrics based on current status
                    let waitTime = "-";
                    let execTime = "-";
                    let totalTime = "-";

                    if (job.status === 'PENDING') {
                        waitTime = formatDuration(now - created) + " (waiting)";
                        execTime = "-";
                        totalTime = "-";
                    } else if (job.status === 'PROCESSING') {
                        waitTime = started ? formatDuration(started - created) : "-";
                        execTime = started ? formatDuration(now - started) + " (running)" : "-";
                        totalTime = formatDuration(now - created);
                    } else {
                        // SUCCESS, FAILED, DEAD
                        waitTime = started ? formatDuration(started - created) : "-";
                        execTime = (completed && started) ? formatDuration(completed - started) : "-";
                        totalTime = completed ? formatDuration(completed - created) : "-";
                    }

                    const tr = document.createElement('tr');
                    const workerBadge = job.worker_id ? `<span style="color:var(--accent-primary)">#${job.worker_id}</span>` : '-';
                    
                    const isRetrying = job.status === 'PENDING' && job.retries > 0;
                    const statusText = isRetrying ? `RETRYING (${job.retries}/${job.max_retries})` : job.status;
                    
                    let extraInfo = `<div style="font-size:0.75rem; color:var(--text-secondary)">${job.type}</div>`;
                    if (isRetrying) {
                        const backoff = (1 << job.retries);
                        extraInfo += `<div style="font-size:0.75rem; color:#ef4444; margin-top:4px;">Wait: ${backoff}s backoff</div>`;
                    } else if (job.retries > 0 && job.status === 'SUCCESS') {
                        extraInfo += `<div style="font-size:0.75rem; color:#10b981; margin-top:4px;">Succeeded on attempt ${job.retries + 1}</div>`;
                    }

                    tr.innerHTML = `
                        <td><code>${shortId}</code>${extraInfo}</td>
                        <td>${workerBadge}</td>
                        <td><span class="badge badge-${job.status}">${statusText}</span></td>
                        <td>${waitTime}</td>
                        <td>${execTime}</td>
                        <td>${totalTime}</td>
                    `;
                    recentJobsBody.appendChild(tr);
                });
            } else {
                recentJobsBody.innerHTML = '<tr><td colspan="6" style="text-align: center; color: var(--text-secondary);">No recent jobs found.</td></tr>';
            }
        } catch (error) {
            console.error('Recent jobs fetch error:', error);
        }
    }

    // Fetch DLQ jobs independently so they never get lost
    async function fetchDLQ() {
        try {
            const [deadRes, failedRes] = await Promise.all([
                fetch('/jobs?status=DEAD'),
                fetch('/jobs?status=FAILED')
            ]);
            
            const deadJobs = deadRes.ok ? await deadRes.json() : [];
            const failedJobs = failedRes.ok ? await failedRes.json() : [];
            const allDlq = [...(deadJobs || []), ...(failedJobs || [])];

            dlqJobsBody.innerHTML = '';

            if (allDlq.length > 0) {
                allDlq.forEach(job => {
                    const shortId = job.id.split('-')[0];
                    const dlqTr = document.createElement('tr');
                    dlqTr.innerHTML = `
                        <td><code>${shortId}</code></td>
                        <td>${job.type}</td>
                        <td style="color: #ef4444; font-size:0.85rem;">${job.error || 'Unknown error'}</td>
                        <td><button class="btn-primary btn-sm retry-btn" data-id="${job.id}">Retry</button></td>
                    `;
                    dlqJobsBody.appendChild(dlqTr);
                });

                // Attach retry listeners
                document.querySelectorAll('.retry-btn').forEach(btn => {
                    btn.addEventListener('click', async (e) => {
                        const id = e.target.dataset.id;
                        e.target.disabled = true;
                        e.target.innerText = '...';
                        try {
                            const res = await fetch(`/jobs/${id}/retry`, { method: 'POST' });
                            if (res.ok) {
                                showToast('Job retried successfully!', 'success');
                                fetchDLQ();
                                fetchStats();
                            } else {
                                showToast('Failed to retry job', 'error');
                            }
                        } catch (err) {
                            showToast('Failed to retry job', 'error');
                        }
                    });
                });
            } else {
                dlqJobsBody.innerHTML = '<tr><td colspan="4" style="text-align: center; color: var(--text-secondary);">No failed jobs! 🎉</td></tr>';
            }
        } catch (error) {
            console.error('DLQ fetch error:', error);
        }
    }

    // Polling every 1 second
    setInterval(() => {
        fetchStats();
        fetchRecentJobs();
        fetchDLQ();
    }, 1000);
    fetchStats();
    fetchRecentJobs();
    fetchDLQ();

    // Scale Workers
    scaleBtn.addEventListener('click', async () => {
        const count = parseInt(document.getElementById('workerCount').value) || 0;
        scaleBtn.disabled = true;
        try {
            const res = await fetch('/workers/scale', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ count })
            });
            if (res.ok) {
                showToast(`Scaled worker pool to ${count}`, 'success');
                addFeedItem(`System dynamically scaled to ${count} workers.`);
            } else {
                showToast('Failed to scale workers', 'error');
            }
        } catch (err) {
            showToast('Failed to scale workers', 'error');
        } finally {
            scaleBtn.disabled = false;
        }
    });

    // Single Job Submit
    singleBtn.addEventListener('click', async () => {
        await submitJobs(1);
    });

    // Burst Form Submit
    jobForm.addEventListener('submit', async (e) => {
        e.preventDefault();
        const count = parseInt(document.getElementById('spamCount').value) || 10;
        await submitJobs(count);
    });

    async function submitJobs(count) {
        const type = document.getElementById('jobType').value;
        const payload = JSON.stringify({ target: "system", timestamp: Date.now() });

        if (count === 1) {
            singleBtn.disabled = true;
            document.getElementById('singleLoader').classList.remove('hidden');
        } else {
            simulateBtn.disabled = true;
            simulateBtn.innerText = 'Spamming...';
            addFeedItem(`Initiating burst of ${count} ${type} jobs...`);
            showToast(`Spamming ${count} jobs to the queue!`, 'success');
        }

        try {
            // Fire them concurrently
            const promises = [];
            for (let i = 0; i < count; i++) {
                promises.push(
                    fetch('/jobs', {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({ type, payload })
                    }).catch(e => console.error(e))
                );
            }
            await Promise.all(promises);
            
            if (count === 1) {
                showToast('Job dispatched successfully!', 'success');
                addFeedItem(`Dispatched 1 job (${type})`);
            } else {
                addFeedItem(`Finished sending ${count} jobs.`);
            }
        } finally {
            singleBtn.disabled = false;
            document.getElementById('singleLoader').classList.add('hidden');
            simulateBtn.disabled = false;
            simulateBtn.innerText = 'Burst Jobs!';
            fetchStats();
            fetchRecentJobs();
        }
    }

    // Purge System
    const purgeBtn = document.getElementById('purgeBtn');
    purgeBtn.addEventListener('click', async () => {
        if (!confirm('Are you sure you want to completely purge the system? This will delete all jobs from Redis and Postgres.')) return;
        
        purgeBtn.disabled = true;
        purgeBtn.innerText = 'Purging...';
        
        try {
            const res = await fetch('/jobs/purge', { method: 'DELETE' });
            if (res.ok) {
                showToast('System completely purged!', 'success');
                addFeedItem('SYSTEM PURGED: All data destroyed.');
            } else {
                showToast('Failed to purge system', 'error');
            }
        } catch (err) {
            showToast('Failed to purge system', 'error');
        } finally {
            purgeBtn.disabled = false;
            purgeBtn.innerText = 'Purge System';
            fetchStats();
            fetchRecentJobs();
        }
    });

    function animateValue(obj, end) {
        const current = parseInt(obj.innerText.replace(/,/g, '')) || 0;
        if (current === end) return;
        obj.innerText = end.toLocaleString();
        obj.style.color = 'white';
        obj.style.transform = 'scale(1.1)';
        setTimeout(() => { obj.style.color = ''; obj.style.transform = 'scale(1)'; }, 200);
    }

    function addFeedItem(message) {
        if (!hasActivity) {
            feedContainer.innerHTML = '';
            hasActivity = true;
        }
        const div = document.createElement('div');
        div.className = 'feed-item';
        const time = new Date().toLocaleTimeString([], { hour12: false, hour: '2-digit', minute:'2-digit', second:'2-digit' });
        div.innerHTML = `<strong>[${time}]</strong> ${message}`;
        feedContainer.prepend(div);
        if (feedContainer.children.length > 100) feedContainer.lastChild.remove();
    }

    function showToast(message, type) {
        const toast = document.createElement('div');
        toast.className = 'toast';
        toast.innerText = message;
        toast.style.borderLeft = type === 'error' ? '4px solid #ef4444' : '4px solid #10b981';
        toastContainer.appendChild(toast);
        setTimeout(() => {
            toast.style.opacity = '0';
            toast.style.transform = 'translateY(20px)';
            setTimeout(() => toast.remove(), 300);
        }, 3000);
    }
});
