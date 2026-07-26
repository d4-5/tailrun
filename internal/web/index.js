(function(){
  "use strict";

  const API = "/api";
  const REFRESH_MS = 1000;
  let cache = { workers: [], tasks: [] };
  let activeLogTaskId = null;
  let logIntervalId = null;

  function fmtBytes(v){
    if (typeof v !== "number") return String(v);
    if (v === 0) return "0 B";
    const units = ["B","KB","MB","GB","TB","PB"];
    const i = Math.min(units.length - 1, Math.floor(Math.log(v) / Math.log(1024)));
    const n = v / Math.pow(1024, i);
    return (n >= 10 ? n.toFixed(0) : n.toFixed(1)) + " " + units[i];
  }

  function statusClass(s){
    return (s || "unknown").toString().toLowerCase().replace(/[^a-z]/g, "");
  }

  function normalizeTaskStatus(status) {
    const s = statusClass(status);
    if (s === "waiting") return "queued";
    if (s === "executing") return "running";
    if (s === "finished") return "success";
    return s;
  }

  function badge(status){
    const cls = normalizeTaskStatus(status);
    const known = ["queued","running","success","failed","error"];
    const c = known.includes(cls) ? cls : "default";
    return `<span class="badge ${c}"><i></i>${escapeHtml(status || "unknown")}</span>`;
  }

  function ledClass(status){
    const cls = statusClass(status);
    if (cls === "available") return "online";
    if (cls === "busy") return "running";
    if (cls === "offline") return "offline";
    return "default";
  }

  function workerStatusTextClass(status) {
    const cls = statusClass(status);
    if (cls === "available") return "online";
    if (cls === "busy") return "running";
    if (cls === "offline") return "offline";
    return "default";
  }

  function escapeHtml(s){
    return String(s ?? "").replace(/[&<>"']/g, m => ({ "&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;" }[m]));
  }

  function coreGrid(cores, big){
    cores = Math.max(1, Math.min(64, cores || 1));
    const cols = big ? Math.min(16, cores) : 8;
    let html = `<div class="core-grid" style="grid-template-columns:repeat(${cols},1fr)">`;
    for (let i = 0; i < cores; i++) html += "<i></i>";
    html += "</div>";
    return html;
  }

  async function fetchJSON(url, opts){
    const res = await fetch(API + url, opts);
    if (!res.ok){
      let msg = res.statusText;
      try { const b = await res.json(); if (b && b.error) msg = b.error; } catch(e){}
      throw new Error(msg || ("HTTP " + res.status));
    }
    if (res.status === 204) return null;
    return res.json();
  }

  async function loadAll(){
    try{
      const [workers, tasks] = await Promise.all([
        fetchJSON("/workers"),
        fetchJSON("/tasks"),
      ]);
      cache.workers = workers || [];
      cache.tasks = tasks || [];
      document.getElementById("liveDot").classList.remove("offline");
      document.getElementById("liveDot").querySelector("span").textContent = "live";
    } catch(err){
      console.error("API error:", err);
      document.getElementById("liveDot").classList.add("offline");
      document.getElementById("liveDot").querySelector("span").textContent = "error";
    }
  }

  function renderStatStrip(){
    const w = cache.workers, t = cache.tasks;
    const online = w.length;
    const queued = t.filter(x => normalizeTaskStatus(x.status) === "queued").length;
    const running = t.filter(x => normalizeTaskStatus(x.status) === "running").length;
    const failed = t.filter(x => ["failed","error"].includes(normalizeTaskStatus(x.status))).length;
    document.getElementById("statStrip").innerHTML = `
      <span><b>${online}</b> workers online</span>
      <span class="divider"></span>
      <span><b>${queued}</b> queued</span>
      <span class="divider"></span>
      <span><b>${running}</b> running</span>
      <span class="divider"></span>
      <span><b>${failed}</b> failed</span>
    `;
  }

  let taskFilter = "all";

  function renderDashboard(){
    const app = document.getElementById("app");
    const w = cache.workers, t = cache.tasks;
    const queue = t.filter(x => normalizeTaskStatus(x.status) === "queued");

    app.innerHTML = `
      <div class="stats-inline">
        <span>${w.length} workers</span><span>${queue.length} queued</span>
        <span>${t.filter(x=>normalizeTaskStatus(x.status)==="running").length} running</span>
      </div>

      <section>
        <div class="section-head">
          <h2>Workers <span class="count">${w.length}</span></h2>
          <span class="hint">click a worker for details</span>
        </div>
        <div class="rail" id="workerRail"></div>
      </section>

      <section>
        <div class="section-head">
          <h2>Queue <span class="count">${queue.length}</span></h2>
          <span class="hint">waiting for a free worker</span>
        </div>
        <div class="rail" id="queueRail"></div>
      </section>

      <section>
        <div class="section-head">
          <h2>All tasks <span class="count">${t.length}</span></h2>
          <select class="filter" id="taskFilterSel">
            <option value="all">All statuses</option>
            <option value="queued">Queued</option>
            <option value="running">Running</option>
            <option value="success">Success</option>
            <option value="failed">Failed</option>
          </select>
        </div>
        <div class="table-wrap" id="taskTableWrap"></div>
      </section>
    `;

    document.getElementById("taskFilterSel").value = taskFilter;
    document.getElementById("taskFilterSel").addEventListener("change", e => {
      taskFilter = e.target.value;
      renderTaskTable();
    });

    renderWorkerRail();
    renderQueueRail();
    renderTaskTable();
  }

  function renderWorkerRail(){
    const el = document.getElementById("workerRail");
    if (!cache.workers.length){
      el.innerHTML = `<div class="empty-slot">No workers registered yet.</div>`;
      return;
    }
    el.innerHTML = cache.workers.map(w => `
      <button class="worker-card" data-worker-id="${w.id}">
        <span class="led ${ledClass(w.status)}"></span>
        <div class="host">${escapeHtml(w.hostname)}</div>
        <div class="os">${escapeHtml(w.os)}</div>
        ${coreGrid(w.cpuCores, false)}
        <div class="specs">
          <span><b>${w.cpuCores}</b> cores</span>
          <span><b>${fmtBytes(w.memory)}</b> mem</span>
        </div>
        <div class="status-label status-text ${workerStatusTextClass(w.status)}">${escapeHtml(w.status)}</div>
      </button>
    `).join("");
    el.querySelectorAll("[data-worker-id]").forEach(btn => {
      btn.addEventListener("click", () => { location.hash = "#/workers/" + btn.dataset.workerId; });
    });
  }

  function renderQueueRail(){
    const el = document.getElementById("queueRail");
    const queue = cache.tasks.filter(t => normalizeTaskStatus(t.status) === "queued");
    if (!queue.length){
      el.innerHTML = `<div class="empty-slot">Queue is empty — nothing waiting on a worker.</div>`;
      return;
    }
    el.innerHTML = queue.map((t, i) => `
      <div class="task-card" style="cursor:pointer;" data-task-id="${t.id}">
        <div class="qpos">#${i + 1} in queue</div>
        <div class="name">${escapeHtml(t.name || "(unnamed)")}</div>
        <div class="cmd" title="${escapeHtml(t.command)}">${escapeHtml(t.command)}</div>
      </div>
    `).join("");
    el.querySelectorAll("[data-task-id]").forEach(card => {
      card.addEventListener("click", () => showTaskDetails(card.dataset.taskId));
    });
  }

  function renderTaskTable(){
    const wrap = document.getElementById("taskTableWrap");
    let rows = cache.tasks;
    if (taskFilter !== "all") {
      rows = rows.filter(t => normalizeTaskStatus(t.status) === taskFilter);
    }

    if (!rows.length){
      wrap.innerHTML = `<div class="empty-state"><b>No tasks here</b>Nothing matches this filter yet.</div>`;
      return;
    }

    const workerById = Object.fromEntries(cache.workers.map(w => [w.id, w]));

    wrap.innerHTML = `
      <table>
        <thead><tr>
          <th>ID</th><th>Name</th><th>Command</th><th>Status</th><th>Worker</th><th></th>
        </tr></thead>
        <tbody>
          ${rows.map(t => `
            <tr style="cursor:pointer;" data-task-row-id="${t.id}">
              <td class="id">${t.id}</td>
              <td style="font-weight:600;">${escapeHtml(t.name || "—")}</td>
              <td class="cmd" title="${escapeHtml(t.command)}">${escapeHtml(t.command)}</td>
              <td>${badge(t.status)}</td>
              <td class="worker-link">${
                t.workerId != null
                  ? `<a href="#/workers/${t.workerId}">${escapeHtml(workerById[t.workerId]?.hostname || ("#" + t.workerId))}</a>`
                  : `<span style="color:var(--text-faint)">—</span>`
              }</td>
              <td>
                <div class="row-actions">
                  <button class="icon-btn" title="View details & logs" data-details-btn-id="${t.id}">☰</button>
                </div>
              </td>
            </tr>
          `).join("")}
        </tbody>
      </table>
    `;

    wrap.querySelectorAll("tr[data-task-row-id]").forEach(tr => {
      tr.addEventListener("click", e => {
        if (e.target.tagName === "A") return;
        showTaskDetails(tr.dataset.taskRowId);
      });
    });
    wrap.querySelectorAll("[data-details-btn-id]").forEach(btn => {
      btn.addEventListener("click", e => {
        e.stopPropagation();
        showTaskDetails(btn.dataset.detailsBtnId);
      });
    });
  }

  async function renderWorkerDetail(id){
    const app = document.getElementById("app");
    let worker = cache.workers.find(w => String(w.id) === String(id));

    if (!worker){
      try { worker = await fetchJSON("/workers/" + id); } catch(e){}
    }

    if (!worker){
      app.innerHTML = `
        <div class="back-link" id="backLink">← back to dashboard</div>
        <div class="empty-state"><b>Worker not found</b>It may have been removed, or the ID is wrong.</div>
      `;
      document.getElementById("backLink").addEventListener("click", () => location.hash = "#/");
      return;
    }

    const tasksOnWorker = cache.tasks.filter(t => String(t.workerId) === String(worker.id));

    app.innerHTML = `
      <div class="back-link" id="backLink">← back to dashboard</div>
      <div class="detail-head">
        <div>
          <h1>${escapeHtml(worker.hostname)}</h1>
          <div class="url">${escapeHtml(worker.url)}</div>
        </div>
        <span class="badge ${ledClass(worker.status)}">
          <i></i>${escapeHtml(worker.status)}
        </span>
      </div>

      <div class="spec-grid">
        <div class="spec-cell"><div class="k">OS</div><div class="v">${escapeHtml(worker.os)}</div></div>
        <div class="spec-cell"><div class="k">CPU cores</div><div class="v">${worker.cpuCores}</div></div>
        <div class="spec-cell"><div class="k">Memory</div><div class="v">${fmtBytes(worker.memory)}</div></div>
        <div class="spec-cell"><div class="k">Storage</div><div class="v">${fmtBytes(worker.storage)}</div></div>
      </div>

      <section style="margin-top:0">
        <div class="section-head"><h2>Capacity</h2></div>
        ${coreGrid(worker.cpuCores, true)}
      </section>

      <section>
        <div class="section-head">
          <h2>Tasks on this worker <span class="count">${tasksOnWorker.length}</span></h2>
        </div>
        <div class="table-wrap" id="workerTaskTable"></div>
      </section>
    `;

    document.getElementById("backLink").addEventListener("click", () => location.hash = "#/");

    const tt = document.getElementById("workerTaskTable");
    if (!tasksOnWorker.length){
      tt.innerHTML = `<div class="empty-state"><b>No tasks yet</b>Nothing has been dispatched to this worker.</div>`;
    } else {
      tt.innerHTML = `
        <table>
          <thead><tr><th>ID</th><th>Name</th><th>Command</th><th>Status</th><th></th></tr></thead>
          <tbody>
            ${tasksOnWorker.map(t => `
              <tr style="cursor:pointer;" data-task-row-id="${t.id}">
                <td class="id">${t.id}</td>
                <td style="font-weight:600;">${escapeHtml(t.name || "—")}</td>
                <td class="cmd" title="${escapeHtml(t.command)}">${escapeHtml(t.command)}</td>
                <td>${badge(t.status)}</td>
                <td>
                  <div class="row-actions">
                    <button class="icon-btn" title="View details & logs" data-details-btn-id="${t.id}">☰</button>
                  </div>
                </td>
              </tr>
            `).join("")}
          </tbody>
        </table>
      `;
      tt.querySelectorAll("tr[data-task-row-id]").forEach(tr => {
        tr.addEventListener("click", () => showTaskDetails(tr.dataset.taskRowId));
      });
      tt.querySelectorAll("[data-details-btn-id]").forEach(btn => {
        btn.addEventListener("click", e => {
          e.stopPropagation();
          showTaskDetails(btn.dataset.detailsBtnId);
        });
      });
    }
  }

  async function fetchTaskLogs(taskId) {
    if (!taskId) return;
    try {
      const data = await fetchJSON("/tasks/" + taskId + "/logs");
      const stdout = data && data.stdout ? data.stdout : "(no stdout output)";
      const stderr = data && data.stderr ? data.stderr : "(no stderr output)";

      const stdoutBody = document.getElementById("stdoutBody");
      const stderrBody = document.getElementById("stderrBody");

      if (stdoutBody) stdoutBody.textContent = stdout;
      if (stderrBody) stderrBody.textContent = stderr;

      const autoScroll = document.getElementById("autoScrollLogs").checked;
      const activeTab = document.querySelector(".log-tab.active");
      if (autoScroll && activeTab) {
        const targetId = activeTab.dataset.tabFor;
        const targetEl = document.getElementById(targetId);
        if (targetEl) {
          targetEl.scrollTop = targetEl.scrollHeight;
        }
      }
    } catch (err) {
      console.error("Error fetching logs:", err);
      const stdoutBody = document.getElementById("stdoutBody");
      const stderrBody = document.getElementById("stderrBody");
      if (stdoutBody) stdoutBody.textContent = "Couldn't load logs: " + err.message;
      if (stderrBody) stderrBody.textContent = "";
    }
  }

  async function showTaskDetails(taskId) {
    activeLogTaskId = taskId;
    if (logIntervalId) {
      clearInterval(logIntervalId);
      logIntervalId = null;
    }

    const overlay = document.getElementById("taskDetailsModal");
    document.getElementById("logsBody").textContent = "loading logs…";
    overlay.classList.add("show");

    try {
      const task = await fetchJSON("/tasks/" + taskId);
      document.getElementById("detailsTaskTitle").textContent = task.name || `Task #${task.id}`;
      document.getElementById("detailsTaskId").textContent = task.id;
      document.getElementById("detailsTaskCommand").textContent = task.command;
      document.getElementById("detailsTaskStatus").textContent = task.status;

      document.querySelectorAll(".log-tab").forEach(t => t.classList.remove("active"));
      document.querySelector(".log-tab[data-tab-for='stdoutBody']").classList.add("active");
      document.getElementById("stdoutBody").style.display = "block";
      document.getElementById("stderrBody").style.display = "none";

      const detailsTaskBadge = document.getElementById("detailsTaskBadge");
      detailsTaskBadge.className = "badge " + normalizeTaskStatus(task.status);
      detailsTaskBadge.innerHTML = `<i></i>${task.status}`;

      const workerById = Object.fromEntries(cache.workers.map(w => [w.id, w]));
      const workerVal = task.workerId != null
        ? (workerById[task.workerId]?.hostname || ("#" + task.workerId))
        : "Unassigned";
      document.getElementById("detailsTaskWorker").textContent = workerVal;

      const envBlock = document.getElementById("detailsEnvBlock");
      if (task.envVars && Object.keys(task.envVars).length > 0) {
        envBlock.style.display = "block";
        document.getElementById("detailsTaskEnv").textContent = Object.entries(task.envVars)
          .map(([k, v]) => `${k}=${v}`)
          .join("\n");
      } else {
        envBlock.style.display = "none";
      }

      await fetchTaskLogs(taskId);

      const normStatus = normalizeTaskStatus(task.status);
      if (normStatus === "queued" || normStatus === "running") {
        logIntervalId = setInterval(async () => {
          await fetchTaskLogs(taskId);
          try {
            const updatedTask = await fetchJSON("/tasks/" + taskId);
            document.getElementById("detailsTaskStatus").textContent = updatedTask.status;
            detailsTaskBadge.className = "badge " + normalizeTaskStatus(updatedTask.status);
            detailsTaskBadge.innerHTML = `<i></i>${updatedTask.status}`;

            const updatedWorkerVal = updatedTask.workerId != null
              ? (workerById[updatedTask.workerId]?.hostname || ("#" + updatedTask.workerId))
              : "Unassigned";
            document.getElementById("detailsTaskWorker").textContent = updatedWorkerVal;

            const updatedNorm = normalizeTaskStatus(updatedTask.status);
            if (updatedNorm !== "queued" && updatedNorm !== "running") {
              clearInterval(logIntervalId);
              logIntervalId = null;
              await loadAll();
              renderStatStrip();
              await route();
            }
          } catch(e){}
        }, 2000);
      }
    } catch(err) {
      console.error("Error loading task details:", err);
      document.getElementById("logsBody").textContent = "Couldn't load task details: " + err.message;
    }
  }

  function closeDetails() {
    document.getElementById("taskDetailsModal").classList.remove("show");
    activeLogTaskId = null;
    if (logIntervalId) {
      clearInterval(logIntervalId);
      logIntervalId = null;
    }
  }

  document.getElementById("closeDetails").addEventListener("click", closeDetails);
  document.getElementById("refreshLogsBtn").addEventListener("click", () => {
    if (activeLogTaskId) fetchTaskLogs(activeLogTaskId);
  });

  document.querySelectorAll(".log-tab").forEach(tab => {
    tab.addEventListener("click", () => {
      document.querySelectorAll(".log-tab").forEach(t => t.classList.remove("active"));
      tab.classList.add("active");
      const targetId = tab.dataset.tabFor;
      document.querySelectorAll(".logs").forEach(el => el.style.display = "none");
      document.getElementById(targetId).style.display = "block";
    });
  });
  document.getElementById("taskDetailsModal").addEventListener("click", e => {
    if (e.target === e.currentTarget) closeDetails();
  });

  const newTaskModal = document.getElementById("newTaskModal");
  function openNewTask(){
    document.getElementById("taskName").value = "";
    document.getElementById("taskCommand").value = "";
    document.getElementById("taskEnv").value = "";
    document.getElementById("newTaskError").classList.remove("show");
    newTaskModal.classList.add("show");
    document.getElementById("taskCommand").focus();
  }
  function closeNewTask(){ newTaskModal.classList.remove("show"); }
  document.getElementById("newTaskBtn").addEventListener("click", openNewTask);
  document.getElementById("closeNewTask").addEventListener("click", closeNewTask);
  document.getElementById("cancelNewTask").addEventListener("click", closeNewTask);
  newTaskModal.addEventListener("click", e => { if (e.target === e.currentTarget) closeNewTask(); });

  document.getElementById("submitNewTask").addEventListener("click", async () => {
    const name = document.getElementById("taskName").value.trim();
    const command = document.getElementById("taskCommand").value.trim();
    const envRaw = document.getElementById("taskEnv").value.trim();
    const errEl = document.getElementById("newTaskError");
    errEl.classList.remove("show");

    if (!command){
      errEl.textContent = "Command is required.";
      errEl.classList.add("show");
      return;
    }

    let envVars = undefined;
    if (envRaw){
      envVars = {};
      for (const line of envRaw.split("\n")){
        const l = line.trim();
        if (!l) continue;
        const idx = l.indexOf("=");
        if (idx === -1){
          errEl.textContent = `Couldn't parse "${l}" — use KEY=value.`;
          errEl.classList.add("show");
          return;
        }
        envVars[l.slice(0, idx).trim()] = l.slice(idx + 1).trim();
      }
    }

    try{
      await fetchJSON("/tasks", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ name, command, envVars }),
      });
      closeNewTask();
      await refresh();
    } catch(err){
      errEl.textContent = "Couldn't create task: " + err.message;
      errEl.classList.add("show");
    }
  });

  async function route(){
    const hash = location.hash || "#/";
    const m = hash.match(/^#\/workers\/([^/]+)$/);
    if (m){
      await renderWorkerDetail(m[1]);
    } else {
      renderDashboard();
    }
  }

  async function refresh(){
    await loadAll();
    renderStatStrip();
    await route();
  }

  window.addEventListener("hashchange", route);
  window.addEventListener("keydown", e => {
    if (e.key === "Escape"){
      closeNewTask();
      closeDetails();
    }
  });

  refresh();
  setInterval(async () => {
    await loadAll();
    renderStatStrip();
    if (!newTaskModal.classList.contains("show")) {
      await route();
    }
  }, REFRESH_MS);
})();
