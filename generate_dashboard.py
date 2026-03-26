"""
generate_dashboard.py
─────────────────────────────────────────────────────────────────────────────
Generates index.html — an interactive Cytoscape.js dependency graph of the
Data Marts & Semantic Layer project, styled after the KTAF Big Board design
system, with live completion status and dependency edges pulled from Asana.

All graph data (sections, tasks, task dependencies) is fetched dynamically
from the Asana project — nothing is hardcoded.

USAGE
─────
  export ASANA_PAT="your-personal-access-token"
  python3 generate_dashboard.py
  open index.html

  Add --dry-run to print a section/task summary without generating the HTML.
"""

import os
import sys
import json
import time
import argparse
import datetime
import asana
from asana.rest import ApiException


# ── CONFIG ────────────────────────────────────────────────────────────────────
PROJECT_GID = "1213735218595734"
OUTPUT_FILE  = "index.html"

# Keywords used to classify a section's tasks by node type.
# Matched case-insensitively against the section name.
SECTION_TYPE_KEYWORDS = {
    "dim":       "dim",
    "dimension": "dim",
    "fact":      "fct",
    "fct":       "fct",
    "cube":      "cube",
    "metric":    "cube",
}


# ── ASANA HELPERS ─────────────────────────────────────────────────────────────

def _make_apis(pat):
    cfg = asana.Configuration()
    cfg.access_token = pat
    client = asana.ApiClient(cfg)
    return asana.SectionsApi(client), asana.TasksApi(client)


def _rate_limited(fn, *args, **kwargs):
    for attempt in range(5):
        try:
            return fn(*args, **kwargs)
        except ApiException as e:
            if e.status == 429:
                wait = 2 ** attempt
                print(f"  ⏳ Rate limited — waiting {wait}s...", file=sys.stderr)
                time.sleep(wait)
            else:
                raise
    raise RuntimeError("Max retries exceeded")


# ── ASANA FETCH ───────────────────────────────────────────────────────────────

def fetch_project_data(pat):
    """
    Returns a dict with keys:
      sections  : list of {gid, name, type}
      tasks     : list of {gid, name, completed, notes, section_gid, type}
      edges     : list of {source, target}   (source/target = task names)
    """
    sections_api, tasks_api = _make_apis(pat)

    # 1. Sections
    raw_sections = list(_rate_limited(
        sections_api.get_sections_for_project,
        PROJECT_GID,
        opts={"opt_fields": "gid,name"},
    ))

    def classify_section(name):
        lower = name.lower()
        for kw, typ in SECTION_TYPE_KEYWORDS.items():
            if kw in lower:
                return typ
        return None

    sections = [
        {"gid": s["gid"], "name": s["name"], "type": classify_section(s["name"])}
        for s in raw_sections
    ]
    print(f"  {len(sections)} sections found.")

    # 2. Tasks per section
    tasks = []
    for sec in sections:
        raw_tasks = list(_rate_limited(
            tasks_api.get_tasks_for_section,
            sec["gid"],
            opts={"opt_fields": "gid,name,completed,notes", "limit": 100},
        ))
        for t in raw_tasks:
            tasks.append({
                "gid":         t["gid"],
                "name":        t["name"],
                "completed":   bool(t.get("completed", False)),
                "notes":       t.get("notes", ""),
                "section_gid": sec["gid"],
                "type":        sec["type"],
            })
        time.sleep(0.15)
    print(f"  {len(tasks)} tasks found.")

    # 3. Dependencies for cube metric tasks (edges = what each cube is blocked by)
    cube_tasks = [t for t in tasks if t["type"] == "cube"]
    name_by_gid = {t["gid"]: t["name"] for t in tasks}

    edges = []
    seen  = set()
    for cube in cube_tasks:
        try:
            deps = list(_rate_limited(
                tasks_api.get_dependencies_for_task,
                cube["gid"],
                opts={"opt_fields": "gid,name"},
            ))
            for dep in deps:
                dep_name = dep.get("name") or name_by_gid.get(dep["gid"], dep["gid"])
                key = (dep_name, cube["name"])
                if key not in seen:
                    seen.add(key)
                    edges.append({"source": dep_name, "target": cube["name"]})
            time.sleep(0.12)
        except ApiException as e:
            print(f"  Warning: could not fetch deps for '{cube['name']}': {e.reason}",
                  file=sys.stderr)

    print(f"  {len(edges)} dependency edges found.")

    # 4. Subtasks for each cube metric task
    print("  Fetching subtasks for cube metrics...")
    for cube in cube_tasks:
        try:
            subs = list(_rate_limited(
                tasks_api.get_subtasks_for_task,
                cube["gid"],
                opts={"opt_fields": "gid,name,completed"},
            ))
            cube["subtasks"] = [
                {"name": s["name"], "done": bool(s.get("completed", False))}
                for s in subs
            ]
            time.sleep(0.1)
        except ApiException as e:
            print(f"  Warning: subtasks unavailable for '{cube['name']}': {e.reason}",
                  file=sys.stderr)
            cube.setdefault("subtasks", [])

    total_subs = sum(len(c.get("subtasks", [])) for c in cube_tasks)
    print(f"  {total_subs} subtasks found across {len(cube_tasks)} cube metrics.")
    return {"sections": sections, "tasks": tasks, "edges": edges}


# ── GRAPH BUILDER ─────────────────────────────────────────────────────────────

def build_graph(project_data):
    """Returns (cytoscape_elements, stats_dict) from fetched Asana data."""
    tasks = project_data["tasks"]
    edges = project_data["edges"]

    dim_tasks  = sorted([t for t in tasks if t["type"] == "dim"],  key=lambda t: t["name"])
    fct_tasks  = sorted([t for t in tasks if t["type"] == "fct"],  key=lambda t: t["name"])
    cube_tasks = sorted([t for t in tasks if t["type"] == "cube"], key=lambda t: t["name"])

    # Preset column positions
    def col_positions(task_list, x, spacing=62):
        n = len(task_list)
        start_y = -(n - 1) * spacing / 2
        return {t["name"]: {"x": x, "y": start_y + i * spacing}
                for i, t in enumerate(task_list)}

    positions = {}
    positions.update(col_positions(dim_tasks,  x=0))
    positions.update(col_positions(fct_tasks,  x=620))
    positions.update(col_positions(cube_tasks, x=1240))

    elements = []

    for t in dim_tasks:
        elements.append({
            "data": {
                "id":     t["name"],
                "label":  t["name"],
                "type":   "dim",
                "status": "complete" if t["completed"] else "pending",
                "desc":   t["notes"] or "Dimension model.",
            },
            "position": positions[t["name"]],
        })

    for t in fct_tasks:
        elements.append({
            "data": {
                "id":     t["name"],
                "label":  t["name"],
                "type":   "fct",
                "status": "complete" if t["completed"] else "pending",
                "desc":   t["notes"] or "Fact model.",
            },
            "position": positions[t["name"]],
        })

    for t in cube_tasks:
        subtasks  = t.get("subtasks", [])
        sub_done  = sum(1 for s in subtasks if s["done"])
        sub_total = len(subtasks)
        label = (f"{t['name']}\n{sub_done}/{sub_total} metrics"
                 if sub_total > 0 else t["name"])
        elements.append({
            "data": {
                "id":        t["name"],
                "label":     label,
                "type":      "cube",
                "status":    "complete" if t["completed"] else "pending",
                "desc":      t["notes"] or "Cube metric / Tableau workbook.",
                "sub_done":  sub_done,
                "sub_total": sub_total,
                "subtasks":  subtasks,
            },
            "position": positions[t["name"]],
        })

    all_ids = {e["data"]["id"] for e in elements}
    for i, edge in enumerate(edges):
        src, tgt = edge["source"], edge["target"]
        if src in all_ids and tgt in all_ids:
            elements.append({"data": {"id": f"e{i}", "source": src, "target": tgt}})

    def count(typ, done):
        return sum(1 for t in tasks if t["type"] == typ and t["completed"] == done)

    stats = {
        "dims_done":   count("dim",  True),
        "dims_total":  len(dim_tasks),
        "fcts_done":   count("fct",  True),
        "fcts_total":  len(fct_tasks),
        "cubes_done":  count("cube", True),
        "cubes_total": len(cube_tasks),
    }
    return elements, stats


# ── HTML TEMPLATE ─────────────────────────────────────────────────────────────
# Placeholders: __ELEMENTS__, __STATS__, __UPDATED__

HTML_TEMPLATE = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1.0"/>
<title>Data Marts & Semantic Layer — Progress</title>
<link rel="preconnect" href="https://fonts.googleapis.com"/>
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin/>
<link href="https://fonts.googleapis.com/css2?family=Barlow+Condensed:wght@500;600;700;800&family=DM+Sans:wght@400;500;600;700&family=JetBrains+Mono:wght@400;500;600&display=swap" rel="stylesheet"/>
<script src="https://cdnjs.cloudflare.com/ajax/libs/cytoscape/3.29.2/cytoscape.min.js"></script>
<script src="https://cdnjs.cloudflare.com/ajax/libs/dagre/0.8.5/dagre.min.js"></script>
<script src="https://cdn.jsdelivr.net/npm/cytoscape-dagre@2.5.0/cytoscape-dagre.min.js"></script>
<style>
*, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }

:root {
  --indigo:  #001E62;
  --white:   #FFFFFF;
  --orange:  #F9A21A;
  --blue:    #57C0E9;
  --green:   #4ADE80;
  --g1: #F4F5F7;
  --g2: #E2E4E9;
  --g3: #9BA3AF;
  --g4: #6B7280;
  --g5: #374151;
  --ff-head: 'Barlow Condensed', sans-serif;
  --ff-body: 'DM Sans', sans-serif;
  --ff-mono: 'JetBrains Mono', monospace;
  --r:    8px;
  --r-lg: 14px;
  --mw:   1200px;
}

html { font-size: 16px; scroll-behavior: smooth; }
body {
  background: var(--white);
  color: var(--g5);
  font-family: var(--ff-body);
  line-height: 1.6;
  -webkit-font-smoothing: antialiased;
}

/* ── Header ── */
header {
  position: sticky; top: 0; z-index: 100;
  background: rgba(255,255,255,0.93);
  backdrop-filter: blur(14px);
  border-bottom: 1px solid var(--g2);
}
.hdr {
  max-width: var(--mw); margin: 0 auto; padding: 0 2rem;
  height: 58px; display: flex; align-items: center; justify-content: space-between;
  gap: 1rem;
}
.brand { display: flex; align-items: center; gap: 0.7rem; }
.brand-mark {
  width: 30px; height: 30px; border-radius: 6px;
  background: var(--indigo);
  display: flex; align-items: center; justify-content: center;
  font-family: var(--ff-head); font-size: 10px; font-weight: 700;
  color: var(--white); letter-spacing: 0.06em; flex-shrink: 0;
}
.brand-name {
  font-family: var(--ff-head); font-size: 1rem; font-weight: 700;
  letter-spacing: 0.07em; text-transform: uppercase; color: var(--indigo);
}
.hdr-right { display: flex; align-items: center; gap: 0.5rem; flex-wrap: wrap; }

/* ── Chips ── */
.chip {
  font-family: var(--ff-mono); font-size: 0.62rem; font-weight: 600;
  letter-spacing: 0.04em; border-radius: 4px;
  padding: 0.2rem 0.6rem; border: 1px solid; white-space: nowrap;
}
.c-blue   { color: var(--blue);   border-color: rgba(87,192,233,0.4);  background: rgba(87,192,233,0.09);  }
.c-orange { color: var(--orange); border-color: rgba(249,162,26,0.4);  background: rgba(249,162,26,0.09);  }
.c-green  { color: #22c55e;       border-color: rgba(34,197,94,0.35);  background: rgba(34,197,94,0.07);   }
.c-muted  { color: var(--g4);     border-color: var(--g2);             background: var(--g1);              }
.c-gold   { color: #b45309;       border-color: rgba(180,83,9,0.3);    background: rgba(251,191,36,0.1);   }

.pulse-dot {
  display: inline-block; width: 6px; height: 6px; border-radius: 50%;
  background: var(--blue); margin-right: 4px; vertical-align: middle;
  animation: pulse 2.2s ease-in-out infinite;
}
@keyframes pulse {
  0%, 100% { opacity: 1; transform: scale(1); }
  50%       { opacity: 0.4; transform: scale(0.75); }
}

/* ── Hero strip ── */
.hero {
  padding: 3.5rem 2rem 3rem;
  border-bottom: 1px solid var(--g2);
  background: var(--white);
  position: relative; overflow: hidden;
}
.hero::before {
  content: '';
  position: absolute; inset: 0;
  background-image:
    linear-gradient(var(--g2) 1px, transparent 1px),
    linear-gradient(90deg, var(--g2) 1px, transparent 1px);
  background-size: 48px 48px;
  opacity: 0.4; pointer-events: none;
}
.hero::after {
  content: '';
  position: absolute; top: -80px; right: -80px;
  width: 360px; height: 360px; border-radius: 50%;
  background: radial-gradient(circle, rgba(249,162,26,0.07) 0%, transparent 70%);
  pointer-events: none;
}
.hero-inner { max-width: var(--mw); margin: 0 auto; position: relative; }
.hero-eyebrow {
  display: inline-flex; align-items: center; gap: 0.45rem;
  font-family: var(--ff-mono); font-size: 0.62rem; font-weight: 600;
  letter-spacing: 0.12em; text-transform: uppercase; color: var(--blue);
  background: rgba(87,192,233,0.08); border: 1px solid rgba(87,192,233,0.22);
  border-radius: 20px; padding: 0.28rem 0.9rem; margin-bottom: 1.4rem;
}
.hero-title {
  font-family: var(--ff-head);
  font-size: clamp(2.8rem, 6vw, 4.8rem);
  font-weight: 800; color: var(--indigo);
  line-height: 0.97; letter-spacing: -0.02em; margin-bottom: 1rem;
}
.hero-title-accent { color: var(--orange); }
.hero-body {
  font-size: 1rem; color: var(--g4); line-height: 1.78;
  max-width: 680px; margin-bottom: 2rem;
}
.stat-row { display: flex; gap: 2rem; flex-wrap: wrap; }
.stat-item { display: flex; flex-direction: column; gap: 0.2rem; }
.stat-value {
  font-family: var(--ff-head); font-size: 2rem; font-weight: 800;
  color: var(--indigo); line-height: 1;
}
.stat-label { font-family: var(--ff-mono); font-size: 0.6rem; font-weight: 600;
  letter-spacing: 0.12em; text-transform: uppercase; color: var(--g3); }

/* ── Graph section (dark indigo) ── */
.sec-graph {
  background: var(--indigo);
  padding: 0;
  border-bottom: 1px solid rgba(255,255,255,0.08);
  position: relative;
}
.graph-toolbar {
  max-width: var(--mw); margin: 0 auto;
  padding: 1rem 2rem 0.75rem;
  display: flex; align-items: center; justify-content: space-between; gap: 1rem;
  flex-wrap: wrap;
}
.graph-title {
  font-family: var(--ff-head); font-size: 1.1rem; font-weight: 700;
  letter-spacing: 0.05em; text-transform: uppercase;
  color: rgba(255,255,255,0.5);
}
.toolbar-btns { display: flex; gap: 0.5rem; align-items: center; }
.btn-icon {
  background: rgba(255,255,255,0.07); border: 1px solid rgba(255,255,255,0.15);
  color: rgba(255,255,255,0.65); border-radius: var(--r);
  padding: 0.35rem 0.85rem; font-family: var(--ff-mono); font-size: 0.65rem;
  font-weight: 600; letter-spacing: 0.06em; cursor: pointer;
  transition: background 0.15s, color 0.15s;
}
.btn-icon:hover { background: rgba(255,255,255,0.13); color: var(--white); }

#cy {
  width: 100%;
  height: calc(100vh - 58px);
  min-height: 560px;
  background: var(--indigo);
  display: block;
}

/* ── Info panel ── */
#info-panel {
  position: fixed; right: -360px; top: 70px;
  width: 330px; max-height: calc(100vh - 90px);
  background: var(--white); border: 1px solid var(--g2);
  border-radius: var(--r-lg); padding: 1.4rem;
  box-shadow: -6px 0 32px rgba(0,0,0,0.18);
  transition: right 0.25s cubic-bezier(0.4,0,0.2,1);
  z-index: 200; overflow-y: auto;
}
#info-panel.open { right: 1.5rem; }
.panel-close {
  position: absolute; top: 0.8rem; right: 0.8rem;
  background: var(--g1); border: 1px solid var(--g2); border-radius: 50%;
  width: 26px; height: 26px; font-size: 0.75rem;
  cursor: pointer; display: flex; align-items: center; justify-content: center;
  color: var(--g4); transition: background 0.15s;
}
.panel-close:hover { background: var(--g2); }
.panel-type-row { display: flex; align-items: center; gap: 0.4rem; margin-bottom: 0.65rem; }
.panel-name {
  font-family: var(--ff-head); font-size: 1.3rem; font-weight: 700;
  color: var(--indigo); line-height: 1.15; margin-bottom: 0.75rem;
  word-break: break-word;
}
.panel-section-label {
  font-family: var(--ff-mono); font-size: 0.58rem; font-weight: 600;
  letter-spacing: 0.12em; text-transform: uppercase; color: var(--orange);
  margin: 0.9rem 0 0.35rem;
}
.panel-desc { font-size: 0.85rem; color: var(--g4); line-height: 1.7; }
.panel-rpt-list {
  list-style: none; display: flex; flex-direction: column; gap: 0.25rem;
  margin-top: 0.35rem;
}
.panel-rpt-list li {
  font-family: var(--ff-mono); font-size: 0.6rem; color: var(--g4);
  background: var(--g1); border: 1px solid var(--g2);
  border-radius: 4px; padding: 0.2rem 0.5rem;
}
.sub-list li { display: flex; align-items: baseline; gap: 0.35rem; }
.sub-list li.done { color: var(--g5); }
.sub-list li.done .sub-icon { color: #22c55e; }
.sub-icon { font-size: 0.65rem; flex-shrink: 0; }
.panel-lsid {
  font-family: var(--ff-mono); font-size: 0.6rem; color: var(--g3);
  word-break: break-all; margin-top: 0.25rem;
}

/* ── Legend ── */
.sec-legend {
  background: var(--indigo);
  padding: 1rem 2rem 1.4rem;
  border-top: 1px solid rgba(255,255,255,0.07);
}
.legend-inner {
  max-width: var(--mw); margin: 0 auto;
  display: flex; align-items: center; gap: 0.5rem; flex-wrap: wrap;
}
.legend-label {
  font-family: var(--ff-mono); font-size: 0.58rem; font-weight: 600;
  letter-spacing: 0.1em; text-transform: uppercase;
  color: rgba(255,255,255,0.28); margin-right: 0.5rem;
}
.chip-dark {
  font-family: var(--ff-mono); font-size: 0.62rem; font-weight: 600;
  letter-spacing: 0.04em; border-radius: 4px;
  padding: 0.2rem 0.6rem; border: 1px solid; white-space: nowrap;
}
.cd-blue   { color: var(--blue);   border-color: rgba(87,192,233,0.4);  background: rgba(87,192,233,0.1);  }
.cd-orange { color: var(--orange); border-color: rgba(249,162,26,0.4);  background: rgba(249,162,26,0.1);  }
.cd-white  { color: rgba(255,255,255,0.85); border-color: rgba(255,255,255,0.25); background: rgba(255,255,255,0.06); }
.cd-muted  { color: rgba(255,255,255,0.35); border-color: rgba(255,255,255,0.12); background: transparent; }
.cd-green  { color: #4ade80; border-color: rgba(74,222,128,0.35); background: rgba(74,222,128,0.08); }
.legend-sep { color: rgba(255,255,255,0.15); font-size: 0.8rem; margin: 0 0.2rem; }

/* ── Footer ── */
footer {
  background: var(--indigo);
  border-top: 1px solid rgba(255,255,255,0.06);
  padding: 1.1rem 2rem;
}
.footer-inner {
  max-width: var(--mw); margin: 0 auto;
  display: flex; align-items: center; justify-content: space-between;
  flex-wrap: wrap; gap: 0.5rem;
}
.footer-txt {
  font-family: var(--ff-mono); font-size: 0.6rem;
  letter-spacing: 0.08em; color: rgba(255,255,255,0.22);
}

/* ── Scroll reveal ── */
.rev { opacity: 0; transform: translateY(18px);
  transition: opacity 0.5s ease, transform 0.5s ease; }
.rev.on { opacity: 1; transform: none; }
.d1 { transition-delay: 0.06s; } .d2 { transition-delay: 0.12s; }
.d3 { transition-delay: 0.18s; } .d4 { transition-delay: 0.24s; }
</style>
</head>
<body>

<!-- ── HEADER ── -->
<header>
  <div class="hdr">
    <div class="brand">
      <div class="brand-mark">DM</div>
      <span class="brand-name">Data Marts &amp; Semantic Layer</span>
    </div>
    <div class="hdr-right" id="hdr-stats">
      <!-- filled by JS -->
    </div>
  </div>
</header>

<!-- ── HERO ── -->
<section class="hero">
  <div class="hero-inner">
    <div class="hero-eyebrow">
      <span class="pulse-dot"></span>
      Semantic Layer Build Progress
    </div>
    <h1 class="hero-title">
      From <span class="hero-title-accent">Models</span><br>to Dashboards
    </h1>
    <p class="hero-body rev d1">
      Each node below is a building block of the data warehouse — dimension models,
      fact models, and the Tableau Cube Metrics that depend on them.
      Edges show which models must ship before a dashboard can go live.
      Click any node to explore its connections.
    </p>
    <div class="stat-row rev d2" id="stat-row">
      <!-- filled by JS -->
    </div>
  </div>
</section>

<!-- ── GRAPH ── -->
<div class="sec-graph">
  <div class="graph-toolbar">
    <span class="graph-title">Dependency Graph</span>
    <div class="toolbar-btns">
      <button class="btn-icon" id="btn-fit">Fit View</button>
      <button class="btn-icon" id="btn-reset">Reset Highlight</button>
    </div>
  </div>
  <div id="cy"></div>
</div>

<!-- ── LEGEND ── -->
<div class="sec-legend">
  <div class="legend-inner">
    <span class="legend-label">Node type</span>
    <span class="chip-dark cd-blue">dim_</span>
    <span class="chip-dark cd-orange">fct_</span>
    <span class="chip-dark cd-white">Cube Metric</span>
    <span class="legend-sep">|</span>
    <span class="legend-label">Status</span>
    <span class="chip-dark cd-green">&#10003; complete</span>
    <span class="chip-dark cd-muted">pending</span>
    <span class="chip-dark" style="color:rgba(255,255,255,0.55);border-color:rgba(255,255,255,0.2);background:rgba(255,255,255,0.04);">pre-existing</span>
  </div>
</div>

<!-- ── INFO PANEL ── -->
<div id="info-panel">
  <button class="panel-close" id="panel-close-btn" title="Close">&#10005;</button>
  <div class="panel-type-row" id="panel-type-row"></div>
  <div class="panel-name" id="panel-name"></div>
  <div class="panel-desc" id="panel-desc"></div>
  <div id="panel-extra"></div>
</div>

<!-- ── FOOTER ── -->
<footer>
  <div class="footer-inner">
    <span class="footer-txt">Data Marts &amp; Semantic Layer &mdash; KTAF</span>
    <span class="footer-txt">Last updated: __UPDATED__</span>
  </div>
</footer>

<script>
const ELEMENTS   = __ELEMENTS__;
const STATS      = __STATS__;
const LAST_UPDATED = "__UPDATED__";

// ── Populate header + hero stats ──────────────────────────────────────────
function pct(done, total) {
  return total ? Math.round(done / total * 100) : 0;
}
function statChip(done, total, cls, label) {
  return `<span class="chip ${cls}">${done}/${total} ${label}</span>`;
}
document.getElementById('hdr-stats').innerHTML = [
  statChip(STATS.dims_done,  STATS.dims_total,  'c-blue',   'dims'),
  statChip(STATS.fcts_done,  STATS.fcts_total,  'c-orange', 'facts'),
  statChip(STATS.cubes_done, STATS.cubes_total, 'c-gold',   'cube metrics'),
].join('');

document.getElementById('stat-row').innerHTML = [
  `<div class="stat-item">
     <span class="stat-value">${STATS.dims_done}<span style="color:var(--g3);font-size:1.2rem">/${STATS.dims_total}</span></span>
     <span class="stat-label">Dimensions</span>
   </div>`,
  `<div class="stat-item">
     <span class="stat-value">${STATS.fcts_done}<span style="color:var(--g3);font-size:1.2rem">/${STATS.fcts_total}</span></span>
     <span class="stat-label">Facts</span>
   </div>`,
  `<div class="stat-item">
     <span class="stat-value">${STATS.cubes_done}<span style="color:var(--g3);font-size:1.2rem">/${STATS.cubes_total}</span></span>
     <span class="stat-label">Cube Metrics</span>
   </div>`,
].join('');

// ── Cytoscape ─────────────────────────────────────────────────────────────
cytoscape.use(cytoscapeDagre);

const BLUE   = '#57C0E9';
const ORANGE = '#F9A21A';
const WHITE  = '#FFFFFF';
const INDIGO = '#001E62';
const GREEN  = '#4ADE80';

const cy = cytoscape({
  container: document.getElementById('cy'),
  elements: ELEMENTS,
  layout: {
    name: 'preset',
    fit: true,
    padding: 80,
  },
  style: [
    // ── Base node ──
    {
      selector: 'node',
      style: {
        'font-family':    "'JetBrains Mono', monospace",
        'font-size':      '8.5px',
        'font-weight':    '600',
        'text-valign':    'center',
        'text-halign':    'center',
        'text-wrap':      'wrap',
        'text-max-width': '160px',
        'width':          '185px',
        'height':         '36px',
        'shape':          'round-rectangle',
        'transition-property': 'opacity',
        'transition-duration': '0.2s',
      }
    },
    // ── Dim nodes ──
    {
      selector: 'node[type="dim"][status="complete"]',
      style: { 'background-color': BLUE, 'color': INDIGO, 'border-width': 0 }
    },
    {
      selector: 'node[type="dim"][status="pending"]',
      style: { 'background-color': 'transparent', 'border-color': BLUE,
               'border-width': '2px', 'color': BLUE, 'border-style': 'dashed' }
    },
    {
      selector: 'node[type="dim"][status="existing"]',
      style: { 'background-color': BLUE, 'color': INDIGO,
               'border-width': 0, 'opacity': 0.45 }
    },
    // ── Fact nodes ──
    {
      selector: 'node[type="fct"][status="complete"]',
      style: { 'background-color': ORANGE, 'color': INDIGO, 'border-width': 0 }
    },
    {
      selector: 'node[type="fct"][status="pending"]',
      style: { 'background-color': 'transparent', 'border-color': ORANGE,
               'border-width': '2px', 'color': ORANGE, 'border-style': 'dashed' }
    },
    {
      selector: 'node[type="fct"][status="existing"]',
      style: { 'background-color': ORANGE, 'color': INDIGO,
               'border-width': 0, 'opacity': 0.45 }
    },
    // ── Cube metric nodes ──
    {
      selector: 'node[type="cube"]',
      style: { 'height': '54px', 'font-size': '8px', 'line-height': '1.4' }
    },
    {
      selector: 'node[type="cube"][status="complete"]',
      style: { 'background-color': WHITE, 'color': INDIGO, 'border-width': 0 }
    },
    {
      selector: 'node[type="cube"][status="pending"]',
      style: { 'background-color': 'transparent',
               'border-color': 'rgba(255,255,255,0.55)', 'border-width': '2px',
               'color': 'rgba(255,255,255,0.55)', 'border-style': 'dashed' }
    },
    {
      selector: 'node[type="cube"][status="existing"]',
      style: { 'background-color': WHITE, 'color': INDIGO,
               'border-width': 0, 'opacity': 0.45 }
    },
    // ── Edges ──
    {
      selector: 'edge',
      style: {
        'line-color':          'rgba(255,255,255,0.1)',
        'target-arrow-color':  'rgba(255,255,255,0.1)',
        'target-arrow-shape':  'triangle',
        'arrow-scale':         0.7,
        'curve-style':         'bezier',
        'width':               1.2,
      }
    },
    // ── Interaction states ──
    { selector: '.cy-dimmed',      style: { 'opacity': 0.06 } },
    { selector: 'node.cy-dimmed',  style: { 'opacity': 0.06 } },
    { selector: 'edge.cy-highlight', style: {
        'line-color':         'rgba(255,255,255,0.65)',
        'target-arrow-color': 'rgba(255,255,255,0.65)',
        'width': 2,
    }},
    { selector: 'node.cy-highlight', style: { 'opacity': 1 } },
  ],
  wheelSensitivity: 0.3,
  minZoom: 0.05,
  maxZoom: 4,
  userPanningEnabled: true,
  userZoomingEnabled: true,
});

// ── Interaction ───────────────────────────────────────────────────────────
function resetHighlight() {
  cy.elements().removeClass('cy-dimmed cy-highlight');
}

cy.on('tap', 'node', function(evt) {
  const node = evt.target;
  const d    = node.data();

  // Highlight subgraph
  cy.elements().addClass('cy-dimmed');
  const hood = node.closedNeighborhood();
  hood.removeClass('cy-dimmed').addClass('cy-highlight');

  // Info panel
  const typeLabel = { dim: 'Dimension', fct: 'Fact', cube: 'Cube Metric' }[d.type];
  const typeClass = { dim: 'c-blue', fct: 'c-orange', cube: 'c-gold' }[d.type];
  const statusLabel = { complete: 'complete', pending: 'pending', existing: 'pre-existing' }[d.status];
  const statusClass = { complete: 'c-green', pending: 'c-muted', existing: 'c-muted' }[d.status];

  document.getElementById('panel-type-row').innerHTML =
    `<span class="chip ${typeClass}">${typeLabel}</span>` +
    `<span class="chip ${statusClass}">${statusLabel}</span>`;

  document.getElementById('panel-name').textContent = d.id;
  document.getElementById('panel-desc').textContent = d.desc || '';

  let extra = '';
  if (d.type === 'cube') {
    if (d.sub_total > 0) {
      const pct = Math.round(d.sub_done / d.sub_total * 100);
      const subItems = d.subtasks.map(s =>
        `<li class="${s.done ? 'done' : ''}">
           <span class="sub-icon">${s.done ? '✓' : '○'}</span>${s.name}
         </li>`
      ).join('');
      extra += `<div class="panel-section-label">Metrics — ${d.sub_done}/${d.sub_total} complete (${pct}%)</div>
        <ul class="panel-rpt-list sub-list">${subItems}</ul>`;
    }
  }
  document.getElementById('panel-extra').innerHTML = extra;
  document.getElementById('info-panel').classList.add('open');
});

cy.on('tap', function(evt) {
  if (evt.target === cy) {
    resetHighlight();
    document.getElementById('info-panel').classList.remove('open');
  }
});

document.getElementById('panel-close-btn').addEventListener('click', function() {
  resetHighlight();
  document.getElementById('info-panel').classList.remove('open');
});

document.getElementById('btn-fit').addEventListener('click', function() {
  cy.fit(80);
});

document.getElementById('btn-reset').addEventListener('click', function() {
  resetHighlight();
  document.getElementById('info-panel').classList.remove('open');
});

// ── Scroll reveal ─────────────────────────────────────────────────────────
const revObs = new IntersectionObserver(entries => {
  entries.forEach(e => { if (e.isIntersecting) e.target.classList.add('on'); });
}, { threshold: 0.1 });
document.querySelectorAll('.rev').forEach(el => revObs.observe(el));
</script>
</body>
</html>
"""


# ── GENERATOR ─────────────────────────────────────────────────────────────────

def generate_html(elements, stats, last_updated):
    return (HTML_TEMPLATE
            .replace("__ELEMENTS__", json.dumps(elements, separators=(',', ':')))
            .replace("__STATS__",    json.dumps(stats,    separators=(',', ':')))
            .replace("__UPDATED__",  last_updated))


# ── MAIN ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Generate semantic layer progress dashboard.")
    parser.add_argument("--dry-run", action="store_true",
                        help="Skip Asana API call; all new tasks shown as pending.")
    args = parser.parse_args()

    pat = os.environ.get("ASANA_PAT")
    if not pat:
        sys.exit("Error: ASANA_PAT environment variable is not set.\n"
                 "Run: export ASANA_PAT='your-token'  then retry.")

    if args.dry_run:
        print("Dry run — printing section/task summary only.")
        project_data = fetch_project_data(pat)
        for sec in project_data["sections"]:
            tasks = [t for t in project_data["tasks"] if t["section_gid"] == sec["gid"]]
            print(f"  [{sec['type'] or '?'}] {sec['name']} — {len(tasks)} tasks")
        return

    print("Fetching project data from Asana...")
    project_data = fetch_project_data(pat)

    print("Building dependency graph...")
    elements, stats = build_graph(project_data)

    last_updated = datetime.datetime.now().strftime("%B %d, %Y at %-I:%M %p")
    html = generate_html(elements, stats, last_updated)

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        f.write(html)

    total_nodes = stats["dims_total"] + stats["fcts_total"] + stats["cubes_total"]
    total_done  = stats["dims_done"]  + stats["fcts_done"]  + stats["cubes_done"]
    print(f"  {stats['dims_done']}/{stats['dims_total']} dims  "
          f"{stats['fcts_done']}/{stats['fcts_total']} facts  "
          f"{stats['cubes_done']}/{stats['cubes_total']} cube metrics  "
          f"({total_done}/{total_nodes} overall)")
    print(f"\n✓  Written to {OUTPUT_FILE}")
    print(f"   Open with: open {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
