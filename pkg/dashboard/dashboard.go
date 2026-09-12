// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package dashboard

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/rakyll/miniate/pkg/runtime"
	"github.com/rakyll/miniate/pkg/store"
)

type Dashboard struct {
	st  *store.Store
	eng *runtime.Engine
}

func NewDashboard(st *store.Store, eng *runtime.Engine) *Dashboard {
	return &Dashboard{
		st:  st,
		eng: eng,
	}
}

func (d *Dashboard) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/dashboard", d.handleDashboardHTML)
	mux.HandleFunc("/api/v1/overview", d.handleOverview)
	mux.HandleFunc("/api/v1/resume", d.handleResume)
	mux.HandleFunc("/api/v1/suspend", d.handleSuspend)
	mux.HandleFunc("/api/v1/delete", d.handleDelete)
	mux.HandleFunc("/api/v1/logs", d.handleLogs)
}

func (d *Dashboard) handleOverview(w http.ResponseWriter, req *http.Request) {
	workers, _ := d.st.ListWorkers()
	actors, _ := d.st.ListActors("")
	templates, _ := d.st.ListActorTemplates("")
	atespaces, _ := d.st.ListAtespaces()
	tags, _ := d.st.ListTags("")

	runningCount := 0
	suspendedCount := 0
	for _, a := range actors {
		if a.Status != nil && a.Status.State == ateapipb.ActorState_ACTOR_STATE_RUNNING {
			runningCount++
		} else {
			suspendedCount++
		}
	}

	assignedWorkers := 0
	for _, wrk := range workers {
		if wrk.Status != nil && wrk.Status.Allocated != nil && wrk.Status.Allocated.Actors > 0 {
			assignedWorkers++
		}
	}

	multiplexRatio := "1.0x"
	if len(workers) > 0 && len(actors) > 0 {
		multiplexRatio = fmt.Sprintf("%.1fx", float64(len(actors))/float64(len(workers)))
	}

	type WorkerItem struct {
		Name          string `json:"name"`
		Status        string `json:"status"`
		AssignedActor string `json:"assigned_actor"`
		IP            string `json:"ip"`
	}

	workerItems := make([]WorkerItem, 0, len(workers))
	for _, wrk := range workers {
		status := "FREE"
		assignedActor := ""
		if wrk.Status != nil && wrk.Status.Allocated != nil && wrk.Status.Allocated.Actors > 0 {
			status = "ASSIGNED"
		}
		for _, a := range actors {
			if a.Status != nil && a.Status.WorkerAssignment != nil && a.Status.WorkerAssignment.WorkerPod == wrk.Metadata.Name {
				tmpl := ""
				if a.ActorTemplate != nil {
					tmpl = a.ActorTemplate.Name
				}
				assignedActor = fmt.Sprintf("%s/%s/%s", a.Metadata.Atespace, tmpl, a.Metadata.Name)
				break
			}
		}
		name := ""
		if wrk.Metadata != nil {
			name = wrk.Metadata.Name
		}
		workerItems = append(workerItems, WorkerItem{
			Name:          name,
			Status:        status,
			AssignedActor: assignedActor,
			IP:            wrk.Ip,
		})
	}

	type ActorItem struct {
		Atespace  string `json:"atespace"`
		Name      string `json:"name"`
		Template  string `json:"template"`
		State     string `json:"state"`
		WorkerPod string `json:"worker_pod"`
		Version   int64  `json:"version"`
	}

	actorItems := make([]ActorItem, 0, len(actors))
	for _, a := range actors {
		atespace := ""
		name := ""
		version := int64(1)
		if a.Metadata != nil {
			atespace = a.Metadata.Atespace
			name = a.Metadata.Name
			version = a.Metadata.Version
		}
		tmpl := ""
		if a.ActorTemplate != nil {
			tmpl = a.ActorTemplate.Name
		}
		state := "SUSPENDED"
		workerPod := ""
		if a.Status != nil {
			if a.Status.State == ateapipb.ActorState_ACTOR_STATE_RUNNING {
				state = "RUNNING"
			} else if a.Status.State == ateapipb.ActorState_ACTOR_STATE_PAUSED {
				state = "PAUSED"
			}
			if a.Status.WorkerAssignment != nil {
				workerPod = a.Status.WorkerAssignment.WorkerPod
			}
		}
		actorItems = append(actorItems, ActorItem{
			Atespace:  atespace,
			Name:      name,
			Template:  tmpl,
			State:     state,
			WorkerPod: workerPod,
			Version:   version,
		})
	}

	resp := map[string]any{
		"stats": map[string]any{
			"total_workers":    len(workers),
			"assigned_workers": assignedWorkers,
			"free_workers":     len(workers) - assignedWorkers,
			"total_actors":     len(actors),
			"running_actors":   runningCount,
			"suspended_actors": suspendedCount,
			"total_templates":  len(templates),
			"total_atespaces":  len(atespaces),
			"total_tags":       len(tags),
			"multiplex_ratio":  multiplexRatio,
		},
		"workers":   workerItems,
		"actors":    actorItems,
		"templates": templates,
		"atespaces": atespaces,
		"tags":      tags,
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

func (d *Dashboard) handleResume(w http.ResponseWriter, req *http.Request) {
	if req.Method != "POST" {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var body struct {
		Atespace string `json:"atespace"`
		Actor    string `json:"actor"`
	}
	if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	act, err := d.eng.ResumeActor(req.Context(), body.Atespace, body.Actor)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(act)
}

func (d *Dashboard) handleSuspend(w http.ResponseWriter, req *http.Request) {
	if req.Method != "POST" {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var body struct {
		Atespace string `json:"atespace"`
		Actor    string `json:"actor"`
	}
	if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	act, snapURI, err := d.eng.SuspendActor(req.Context(), body.Atespace, body.Actor)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"actor":        act,
		"snapshot_uri": snapURI,
	})
}

func (d *Dashboard) handleDelete(w http.ResponseWriter, req *http.Request) {
	if req.Method != "POST" {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var body struct {
		Atespace string `json:"atespace"`
		Actor    string `json:"actor"`
	}
	if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	// Suspend first if running to free physical worker slot
	_, _, _ = d.eng.SuspendActor(req.Context(), body.Atespace, body.Actor)
	act, err := d.st.DeleteActor(body.Atespace, body.Actor, true)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(act)
}

func (d *Dashboard) handleLogs(w http.ResponseWriter, req *http.Request) {
	atespace := req.URL.Query().Get("atespace")
	actor := req.URL.Query().Get("actor")
	logs := d.st.GetLogs(atespace, actor)

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"atespace": atespace,
		"actor":    actor,
		"logs":     logs,
	})
}

func (d *Dashboard) handleDashboardHTML(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write([]byte(dashboardHTML))
}

const dashboardHTML = `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Miniate Dashboard</title>
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
  <link href="https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;600&family=Outfit:wght@400;500;600;700&display=swap" rel="stylesheet">
  <style>
    :root {
      --bg-color: #f8fafc;
      --card-bg: #ffffff;
      --card-border: #e2e8f0;
      --card-shadow: 0 1px 3px 0 rgba(0, 0, 0, 0.05), 0 1px 2px -1px rgba(0, 0, 0, 0.05);
      --accent-cyan: #0284c7;
      --accent-purple: #7c3aed;
      --accent-emerald: #059669;
      --accent-rose: #e11d48;
      --text-main: #0f172a;
      --text-muted: #64748b;
    }
    * { box-sizing: border-box; margin: 0; padding: 0; }
    body {
      background: var(--bg-color);
      color: var(--text-main);
      font-family: 'Outfit', sans-serif;
      min-height: 100vh;
      padding: 16px 20px;
    }
    header {
      display: flex;
      justify-content: space-between;
      align-items: center;
      margin-bottom: 14px;
      padding-bottom: 10px;
      border-bottom: 1px solid var(--card-border);
    }
    .logo-container {
      display: flex;
      align-items: center;
      gap: 10px;
    }
    .logo-badge {
      background: linear-gradient(135deg, var(--accent-cyan), var(--accent-purple));
      color: white;
      font-weight: 700;
      padding: 4px 8px;
      border-radius: 6px;
      font-size: 0.95rem;
      letter-spacing: 0.5px;
      box-shadow: 0 2px 4px rgba(2, 132, 199, 0.2);
    }
    .title {
      font-size: 1.2rem;
      font-weight: 700;
      color: #0f172a;
    }
    .subtitle {
      font-size: 0.8rem;
      color: var(--text-muted);
    }
    .live-pulse {
      display: inline-block;
      width: 7px;
      height: 7px;
      border-radius: 50%;
      background: var(--accent-emerald);
      box-shadow: 0 0 6px var(--accent-emerald);
      margin-right: 5px;
      animation: pulse 2s infinite;
    }
    @keyframes pulse {
      0% { opacity: 0.4; }
      50% { opacity: 1; }
      100% { opacity: 0.4; }
    }
    .grid-stats {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(160px, 1fr));
      gap: 10px;
      margin-bottom: 14px;
    }
    .stat-card {
      background: var(--card-bg);
      border: 1px solid var(--card-border);
      border-radius: 8px;
      padding: 10px 12px;
      position: relative;
      overflow: hidden;
      box-shadow: var(--card-shadow);
    }
    .stat-card::before {
      content: '';
      position: absolute;
      top: 0; left: 0; right: 0; height: 3px;
      background: linear-gradient(90deg, var(--accent-cyan), var(--accent-purple));
    }
    .stat-label { font-size: 0.72rem; color: var(--text-muted); text-transform: uppercase; letter-spacing: 0.5px; font-weight: 600; }
    .stat-value { font-size: 1.4rem; font-weight: 700; margin-top: 2px; color: var(--text-main); }
    .stat-sub { font-size: 0.7rem; color: var(--text-muted); margin-top: 2px; }

    .section-title {
      font-size: 0.95rem;
      font-weight: 600;
      margin-bottom: 8px;
      display: flex;
      align-items: center;
      gap: 6px;
      color: var(--text-main);
    }
    .main-layout {
      display: grid;
      grid-template-columns: 2fr 1fr;
      gap: 14px;
    }
    @media (max-width: 1024px) {
      .main-layout { grid-template-columns: 1fr; }
    }
    .card {
      background: var(--card-bg);
      border: 1px solid var(--card-border);
      border-radius: 8px;
      padding: 14px;
      margin-bottom: 14px;
      box-shadow: var(--card-shadow);
    }
    .workers-grid {
      display: grid;
      grid-template-columns: repeat(auto-fill, minmax(95px, 1fr));
      gap: 6px;
    }
    .worker-slot {
      background: #f8fafc;
      border: 1px solid var(--card-border);
      border-radius: 5px;
      padding: 5px 4px;
      text-align: center;
      transition: all 0.2s ease;
    }
    .worker-slot.assigned {
      border-color: var(--accent-cyan);
      background: rgba(2, 132, 199, 0.06);
      box-shadow: 0 0 6px rgba(2, 132, 199, 0.1);
    }
    .worker-name { font-weight: 600; font-size: 0.76rem; color: var(--text-main); }
    .worker-badge {
      display: inline-block;
      font-size: 0.6rem;
      font-weight: 600;
      padding: 1px 4px;
      border-radius: 3px;
      margin-top: 2px;
      text-transform: uppercase;
      line-height: 1.2;
    }
    .worker-badge.free { background: rgba(5, 150, 105, 0.12); color: var(--accent-emerald); }
    .worker-badge.assigned { background: rgba(2, 132, 199, 0.12); color: var(--accent-cyan); }
    .worker-actor { font-size: 0.66rem; color: var(--text-muted); margin-top: 2px; word-break: break-all; }

    table {
      width: 100%;
      border-collapse: collapse;
      font-size: 0.84rem;
    }
    th {
      text-align: left;
      padding: 5px 6px;
      color: var(--text-muted);
      border-bottom: 1px solid var(--card-border);
      font-weight: 600;
      font-size: 0.78rem;
    }
    td {
      padding: 6px 6px;
      border-bottom: 1px solid #f1f5f9;
      color: var(--text-main);
    }
    th:first-child, td:first-child {
      padding-left: 0;
    }
    th:last-child, td:last-child {
      padding-right: 0;
    }
    tr.actor-row {
      cursor: pointer;
      transition: background-color 0.15s ease;
    }
    tr.actor-row:hover {
      background-color: #f8fafc;
    }
    tr.actor-row.selected {
      background-color: #f0f9ff;
    }
    tr.actor-row.selected td {
      border-bottom-color: #bae6fd;
    }
    tr.actor-row.selected td:first-child strong {
      color: var(--accent-cyan);
    }
    .badge {
      display: inline-block;
      padding: 2px 6px;
      border-radius: 4px;
      font-size: 0.72rem;
      font-weight: 600;
      font-family: 'JetBrains Mono', monospace;
    }
    .badge-running { background: rgba(5, 150, 105, 0.1); color: var(--accent-emerald); border: 1px solid rgba(5, 150, 105, 0.2); }
    .badge-suspended { background: rgba(100, 116, 139, 0.1); color: var(--text-muted); border: 1px solid rgba(100, 116, 139, 0.2); }
    .badge-paused { background: rgba(217, 119, 6, 0.1); color: #d97706; border: 1px solid rgba(217, 119, 6, 0.2); }

    .btn {
      display: inline-flex;
      align-items: center;
      justify-content: center;
      background: #ffffff;
      color: #334155;
      border: 1px solid #cbd5e1;
      width: 72px;
      height: 26px;
      box-sizing: border-box;
      border-radius: 5px;
      font-size: 0.76rem;
      cursor: pointer;
      font-family: 'Outfit', sans-serif;
      font-weight: 600;
      letter-spacing: 0.01em;
      transition: all 0.18s cubic-bezier(0.4, 0, 0.2, 1);
      box-shadow: 0 1px 2px rgba(0, 0, 0, 0.03);
      user-select: none;
      line-height: 1;
    }
    .btn:hover {
      background: #f8fafc;
      border-color: #94a3b8;
      color: #0f172a;
      transform: translateY(-1px);
      box-shadow: 0 2px 4px rgba(0, 0, 0, 0.05);
    }
    .btn:active {
      transform: translateY(0);
      box-shadow: none;
    }
    .btn-resume {
      background: #f1f5f9;
      color: #1e293b;
      border-color: #cbd5e1;
    }
    .btn-resume:hover {
      background: #e2e8f0;
      color: #0f172a;
      border-color: #94a3b8;
      box-shadow: 0 2px 4px rgba(0, 0, 0, 0.06);
    }
    .btn-suspend {
      background: #f8fafc;
      color: #475569;
      border-color: #cbd5e1;
    }
    .btn-suspend:hover {
      background: #f1f5f9;
      color: #0f172a;
      border-color: #94a3b8;
      box-shadow: 0 2px 4px rgba(0, 0, 0, 0.06);
    }
    .btn-delete {
      background: #fef2f2;
      color: #dc2626;
      border-color: #fecaca;
    }
    .btn-delete:hover {
      background: #fee2e2;
      color: #991b1b;
      border-color: #fca5a5;
      box-shadow: 0 2px 4px rgba(220, 38, 38, 0.12);
    }
    .btn-copy {
      width: auto;
      height: auto;
      background: rgba(255, 255, 255, 0.08);
      color: #94a3b8;
      border: 1px solid rgba(255, 255, 255, 0.14);
      backdrop-filter: blur(4px);
      font-size: 0.68rem;
      padding: 3px 8px;
      border-radius: 4px;
      box-shadow: none;
    }
    .btn-copy:hover {
      background: rgba(255, 255, 255, 0.18);
      color: #ffffff;
      border-color: rgba(255, 255, 255, 0.28);
    }

    .log-box {
      background: #0f172a;
      border: 1px solid #1e293b;
      border-radius: 6px;
      padding: 10px;
      font-family: 'JetBrains Mono', monospace;
      font-size: 0.78rem;
      height: 190px;
      overflow-y: auto;
      color: #e2e8f0;
      white-space: pre-wrap;
    }
    .interactive-box {
      display: flex;
      flex-direction: column;
      gap: 8px;
    }
  </style>
</head>
<body>
  <header>
    <div class="logo-container">
      <div class="logo-badge">MINIATE</div>
      <div>
        <div class="title">Miniate Dashboard</div>
        <div class="subtitle"><span class="live-pulse"></span>Connected to local control plane & router</div>
      </div>
    </div>
    <div>
      <span style="font-size: 0.82rem; color: var(--text-muted); font-family: 'JetBrains Mono', monospace;">Control Plane :8080 &nbsp;|&nbsp; Router :8000</span>
    </div>
  </header>

  <div class="grid-stats">
    <div class="stat-card">
      <div class="stat-label">Workers</div>
      <div class="stat-value" id="val-workers">0 / 0</div>
      <div class="stat-sub" id="val-workers-sub">0 Assigned • 0 Free</div>
    </div>
    <div class="stat-card">
      <div class="stat-label">Total Actors</div>
      <div class="stat-value" id="val-actors">0</div>
      <div class="stat-sub" id="val-actors-sub">0 Running • 0 Suspended</div>
    </div>
    <div class="stat-card">
      <div class="stat-label">Multiplexing Ratio</div>
      <div class="stat-value" id="val-ratio" style="color: var(--accent-cyan);">1.0x</div>
      <div class="stat-sub">Actors to Physical Workers</div>
    </div>
    <div class="stat-card">
      <div class="stat-label">Atespaces & Templates</div>
      <div class="stat-value" id="val-namespaces">0 / 0</div>
      <div class="stat-sub">Active namespaces</div>
    </div>
  </div>

  <div class="main-layout">
    <div>
      <div class="card">
        <div class="section-title">Physical Worker Pool</div>
        <div class="workers-grid" id="workers-container"></div>
      </div>

      <div class="card">
        <div class="section-title">Actors</div>
        <table>
          <thead>
            <tr>
              <th>Atespace / Name</th>
              <th>Template</th>
              <th>State</th>
              <th>Worker Pod</th>
              <th>Actions</th>
            </tr>
          </thead>
          <tbody id="actors-tbody"></tbody>
        </table>
      </div>
    </div>

    <div>
      <div class="card">
        <div class="section-title">Endpoint</div>
        <div class="interactive-box">
          <p style="font-size: 0.8rem; color: var(--text-muted); margin: 0 0 4px 0;">
            Send requests to actors through the <code>atenet</code> smart ingress router (port 8000). Suspended actors auto-resume transparently:
          </p>
          <div style="position: relative;">
            <pre id="curl-snippet" style="background: #0f172a; border: 1px solid #1e293b; border-radius: 6px; padding: 10px 12px; font-family: 'JetBrains Mono', monospace; font-size: 0.78rem; color: #38bdf8; overflow-x: auto; margin: 0; line-height: 1.4;">curl -X POST \
  -H "ate-target-actor: &lt;atespace&gt;/&lt;actor&gt;" \
  http://localhost:8000/</pre>
            <button class="btn btn-copy" id="copy-btn" onclick="copyCurl()" style="position: absolute; top: 6px; right: 6px;">Copy</button>
          </div>
        </div>
      </div>

      <div class="card">
        <div class="section-title" style="justify-content: space-between;">
          <span>Actor Logs</span>
          <span id="log-target-label" style="font-size: 0.72rem; color: var(--text-muted); font-family: 'JetBrains Mono', monospace;"></span>
        </div>
        <div class="log-box" id="log-content">Select an actor to view logs...</div>
      </div>
    </div>
  </div>

  <script>
    let selectedAtespace = 'default';
    let selectedActor = '';

    async function loadData() {
      try {
        const res = await fetch('/api/v1/overview');
        const data = await res.json();
        renderStats(data.stats);
        renderWorkers(data.workers);
        renderActors(data.actors);
      } catch (err) {
        console.error('Failed to fetch dashboard overview:', err);
      }
    }

    function renderStats(stats) {
      document.getElementById('val-workers').textContent = stats.assigned_workers + ' / ' + stats.total_workers;
      document.getElementById('val-workers-sub').textContent = stats.assigned_workers + ' Assigned • ' + stats.free_workers + ' Free';
      document.getElementById('val-actors').textContent = stats.total_actors;
      document.getElementById('val-actors-sub').textContent = stats.running_actors + ' Running • ' + stats.suspended_actors + ' Suspended';
      document.getElementById('val-ratio').textContent = stats.multiplex_ratio;
      document.getElementById('val-namespaces').textContent = stats.total_atespaces + ' / ' + stats.total_templates;
    }

    function renderWorkers(workers) {
      const container = document.getElementById('workers-container');
      container.innerHTML = '';
      if (!workers || workers.length === 0) {
        container.innerHTML = '<div style="color: var(--text-muted); font-size: 0.85rem;">No workers registered.</div>';
        return;
      }
      workers.sort((a, b) => (a.name || '').localeCompare(b.name || '', undefined, { numeric: true, sensitivity: 'base' }));
      workers.forEach(w => {
        const isAssigned = (w.status === 'ASSIGNED');
        const div = document.createElement('div');
        div.className = 'worker-slot ' + (isAssigned ? 'assigned' : '');
        div.innerHTML =
          '<div class="worker-name">' + w.name + '</div>' +
          '<span class="worker-badge ' + (isAssigned ? 'assigned' : 'free') + '">' + w.status + '</span>' +
          '<div class="worker-actor">' + (w.assigned_actor || '&mdash;') + '</div>';
        container.appendChild(div);
      });
    }

    function renderActors(actors) {
      const tbody = document.getElementById('actors-tbody');
      tbody.innerHTML = '';
      if (!actors || actors.length === 0) {
        tbody.innerHTML = '<tr><td colspan="5" style="text-align: center; color: var(--text-muted);">No actors created yet. Use <code>miniate actor create</code> to get started.</td></tr>';
        return;
      }
      actors.sort((a, b) => {
        const cmpAte = (a.atespace || '').localeCompare(b.atespace || '');
        if (cmpAte !== 0) return cmpAte;
        return (a.name || '').localeCompare(b.name || '');
      });
      actors.forEach(a => {
        const isSelected = (a.atespace === selectedAtespace && a.name === selectedActor);
        const tr = document.createElement('tr');
        tr.className = 'actor-row' + (isSelected ? ' selected' : '');
        tr.setAttribute('data-target', a.atespace + '/' + a.name);
        tr.onclick = () => selectActor(a.atespace, a.name);

        const stateStr = a.state;
        const stateBadgeClass = stateStr === 'RUNNING' ? 'badge-running' : (stateStr === 'PAUSED' ? 'badge-paused' : 'badge-suspended');

        let actions = '<div style="display: inline-flex; gap: 8px; align-items: center;">';
        if (stateStr !== 'RUNNING') {
          actions += '<button class="btn btn-resume" onclick="event.stopPropagation(); resumeActor(\'' + a.atespace + '\', \'' + a.name + '\')">Resume</button>';
        }
        if (stateStr === 'RUNNING') {
          actions += '<button class="btn btn-suspend" onclick="event.stopPropagation(); suspendActor(\'' + a.atespace + '\', \'' + a.name + '\')">Suspend</button>';
        }
        actions += '<button class="btn btn-delete" onclick="event.stopPropagation(); deleteActor(\'' + a.atespace + '\', \'' + a.name + '\')">Delete</button></div>';

        tr.innerHTML =
          '<td><strong>' + a.atespace + '</strong>/<span style="color: var(--accent-cyan); font-weight: 500;">' + a.name + '</span></td>' +
          '<td>' + (a.template || '&mdash;') + '</td>' +
          '<td><span class="badge ' + stateBadgeClass + '">' + stateStr + '</span></td>' +
          '<td style="font-family: \'JetBrains Mono\', monospace; font-size: 0.8rem;">' + (a.worker_pod || '&mdash;') + '</td>' +
          '<td>' + actions + '</td>';
        tbody.appendChild(tr);
      });
      if (!selectedActor && actors.length > 0) {
        selectActor(actors[0].atespace, actors[0].name);
      } else if (actors.length === 0) {
        updateCurlSnippet('', '');
      }
    }

    async function resumeActor(atespace, actor) {
      await fetch('/api/v1/resume', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ atespace, actor })
      });
      loadData();
      loadLogs(atespace, actor);
    }

    async function suspendActor(atespace, actor) {
      await fetch('/api/v1/suspend', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ atespace, actor })
      });
      loadData();
      loadLogs(atespace, actor);
    }

    async function deleteActor(atespace, actor) {
      if (!confirm('Are you sure you want to delete actor ' + atespace + '/' + actor + '?')) {
        return;
      }
      try {
        const res = await fetch('/api/v1/delete', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ atespace, actor })
        });
        if (!res.ok) {
          const errText = await res.text();
          alert('Failed to delete actor: ' + errText);
          return;
        }
        if (selectedAtespace === atespace && selectedActor === actor) {
          selectedActor = null;
          updateCurlSnippet('', '');
          document.getElementById('log-content').textContent = '(Select an actor to view logs)';
          document.getElementById('log-target-label').textContent = 'None';
        }
        loadData();
      } catch (err) {
        alert('Failed to delete actor: ' + err.message);
      }
    }

    function selectActor(atespace, actor) {
      selectedAtespace = atespace;
      selectedActor = actor;
      updateCurlSnippet(atespace, actor);
      document.getElementById('log-target-label').textContent = (atespace && actor) ? (atespace + '/' + actor) : '';
      loadLogs(atespace, actor);

      // Highlight active row in real time
      const target = atespace + '/' + actor;
      document.querySelectorAll('#actors-tbody tr').forEach(row => {
        if (row.getAttribute('data-target') === target) {
          row.classList.add('selected');
        } else {
          row.classList.remove('selected');
        }
      });
    }

    function updateCurlSnippet(atespace, actor) {
      const target = (atespace && actor) ? (atespace + '/' + actor) : '<atespace>/<actor>';
      const el = document.getElementById('curl-snippet');
      if (el) {
        el.textContent = 'curl -X POST \\\n  -H "ate-target-actor: ' + target + '" \\\n  http://localhost:8000/';
      }
    }

    function copyCurl() {
      const el = document.getElementById('curl-snippet');
      const btn = document.getElementById('copy-btn');
      if (el) {
        navigator.clipboard.writeText(el.textContent).then(() => {
          if (btn) {
            btn.textContent = 'Copied!';
            setTimeout(() => { btn.textContent = 'Copy'; }, 1500);
          }
        });
      }
    }

    async function loadLogs(atespace, actor) {
      if (!actor) return;
      try {
        const res = await fetch('/api/v1/logs?atespace=' + encodeURIComponent(atespace) + '&actor=' + encodeURIComponent(actor));
        const data = await res.json();
        const logBox = document.getElementById('log-content');
        logBox.textContent = (data.logs && data.logs.length > 0) ? data.logs.join('\n') : '(No logs recorded)';
        logBox.scrollTop = logBox.scrollHeight;
      } catch (err) {
        console.error(err);
      }
    }

    loadData();
    setInterval(loadData, 2000);
    setInterval(() => {
      if (selectedActor) loadLogs(selectedAtespace, selectedActor);
    }, 2000);
  </script>
</body>
</html>
`
