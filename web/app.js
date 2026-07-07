document.addEventListener('DOMContentLoaded', () => {
    const jobForm = document.getElementById('jobForm');
    const submitBtn = document.getElementById('submitBtn');
    const btnText = submitBtn.querySelector('.btn-text');
    const submitLoader = document.getElementById('submitLoader');
    const simulateBtn = document.getElementById('simulateBtn');
    const feedContainer = document.getElementById('feedContainer');
    const toastContainer = document.getElementById('toastContainer');

    // DOM Elements for Stats
    const statElements = {
        pending: document.getElementById('stat-pending'),
        processing: document.getElementById('stat-processing'),
        success: document.getElementById('stat-success'),
        failed: document.getElementById('stat-failed')
    };

    let hasActivity = false;

    // Fetch Stats Loop
    async function fetchStats() {
        try {
            const res = await fetch('/stats');
            if (!res.ok) throw new Error('Failed to fetch stats');
            const data = await res.json();
            
            // "pending_in_redis" + "dead_in_redis" is raw queue
            // "historical" map contains postgres counts: pending, processing, success, failed
            
            const hist = data.historical || {};
            
            // Use historical as source of truth for UI (it's what postgres sees)
            const pending = hist['PENDING'] || 0;
            const processing = hist['PROCESSING'] || 0;
            const success = hist['SUCCESS'] || 0;
            const failed = (hist['FAILED'] || 0) + (hist['DEAD'] || 0);

            animateValue(statElements.pending, pending);
            animateValue(statElements.processing, processing);
            animateValue(statElements.success, success);
            animateValue(statElements.failed, failed);

        } catch (error) {
            console.error('Stats fetch error:', error);
        }
    }

    // Polling every 1 second
    setInterval(fetchStats, 1000);
    fetchStats();

    // Number animation helper
    function animateValue(obj, end) {
        const current = parseInt(obj.innerText.replace(/,/g, '')) || 0;
        if (current === end) return;
        
        // Simple instant update with a flash effect if it changed
        obj.innerText = end.toLocaleString();
        
        // Flash effect
        obj.style.color = 'white';
        obj.style.transform = 'scale(1.1)';
        setTimeout(() => {
            obj.style.color = '';
            obj.style.transform = 'scale(1)';
        }, 200);
    }

    // Submit single job
    jobForm.addEventListener('submit', async (e) => {
        e.preventDefault();
        
        const type = document.getElementById('jobType').value;
        const payload = document.getElementById('jobPayload').value || "{}";

        setLoading(true);
        try {
            const res = await fetch('/jobs', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ type, payload })
            });
            
            if (!res.ok) throw new Error(await res.text());
            
            const data = await res.json();
            showToast('Job dispatched successfully!', 'success');
            addFeedItem(`Dispatched job: ${data.id.substring(0,8)}... (${type})`);
            
            // clear payload for next time
            document.getElementById('jobPayload').value = '';
        } catch (err) {
            showToast('Failed to dispatch job', 'error');
            console.error(err);
        } finally {
            setLoading(false);
            fetchStats();
        }
    });

    // Spam 100 Jobs
    simulateBtn.addEventListener('click', async () => {
        const type = document.getElementById('jobType').value;
        const payload = document.getElementById('jobPayload').value || "{}";
        
        simulateBtn.disabled = true;
        simulateBtn.innerText = 'Spamming...';
        
        addFeedItem(`Initiating burst of 100 ${type} jobs...`);
        showToast('Spamming 100 jobs to the queue!', 'success');
        
        // Fire and forget 100 requests
        for(let i=0; i<100; i++) {
            fetch('/jobs', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ type, payload: payload })
            }).catch(e => console.error(e));
        }
        
        setTimeout(() => {
            simulateBtn.disabled = false;
            simulateBtn.innerText = 'Spam 100 Jobs';
            addFeedItem(`Finished sending 100 jobs.`);
        }, 1000);
    });

    function setLoading(isLoading) {
        submitBtn.disabled = isLoading;
        if (isLoading) {
            btnText.classList.add('hidden');
            submitLoader.classList.remove('hidden');
        } else {
            btnText.classList.remove('hidden');
            submitLoader.classList.add('hidden');
        }
    }

    function addFeedItem(message) {
        if (!hasActivity) {
            feedContainer.innerHTML = ''; // clear empty message
            hasActivity = true;
        }
        
        const div = document.createElement('div');
        div.className = 'feed-item';
        
        const time = new Date().toLocaleTimeString([], { hour12: false, hour: '2-digit', minute:'2-digit', second:'2-digit' });
        div.innerHTML = `<strong>[${time}]</strong> ${message}`;
        
        feedContainer.prepend(div);
        
        // Keep only last 100 items
        if (feedContainer.children.length > 100) {
            feedContainer.lastChild.remove();
        }
    }

    function showToast(message, type) {
        const toast = document.createElement('div');
        toast.className = 'toast';
        toast.innerText = message;
        
        if (type === 'error') {
            toast.style.borderLeft = '4px solid #ef4444';
        } else {
            toast.style.borderLeft = '4px solid #10b981';
        }

        toastContainer.appendChild(toast);
        
        setTimeout(() => {
            toast.style.opacity = '0';
            toast.style.transform = 'translateY(20px)';
            setTimeout(() => toast.remove(), 300);
        }, 3000);
    }
});
