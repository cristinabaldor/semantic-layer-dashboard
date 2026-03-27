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
<style>
*, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }

:root {
  --indigo:  #001E62;
  --white:   #FFFFFF;
  --orange:  #F9A21A;
  --blue:    #57C0E9;
  --green:   #22c55e;
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
  --mw:   1280px;
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
  background: rgba(255,255,255,0.95);
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
.c-blue   { color: #0c6b96;   border-color: rgba(87,192,233,0.5);   background: rgba(87,192,233,0.12);  }
.c-orange { color: #92400e;   border-color: rgba(249,162,26,0.5);   background: rgba(249,162,26,0.12);  }
.c-green  { color: #166534;   border-color: rgba(34,197,94,0.45);   background: rgba(34,197,94,0.10);   }
.c-muted  { color: var(--g4); border-color: var(--g2);              background: var(--g1);              }
.c-gold   { color: #92400e;   border-color: rgba(180,83,9,0.35);    background: rgba(251,191,36,0.12);  }
.c-red    { color: #991b1b;   border-color: rgba(239,68,68,0.45);   background: rgba(239,68,68,0.09);   }

.pulse-dot {
  display: inline-block; width: 6px; height: 6px; border-radius: 50%;
  background: #0c6b96; margin-right: 4px; vertical-align: middle;
  animation: pulse 2.2s ease-in-out infinite;
}
@keyframes pulse {
  0%, 100% { opacity: 1; transform: scale(1); }
  50%       { opacity: 0.4; transform: scale(0.75); }
}

/* ── Hero ── */
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
  letter-spacing: 0.12em; text-transform: uppercase; color: #0c6b96;
  background: rgba(87,192,233,0.08); border: 1px solid rgba(87,192,233,0.25);
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
/* hero two-column layout */
.hero-layout {
  display: flex; align-items: flex-start; gap: 2.5rem; margin-bottom: 2.5rem;
}
.hero-left { flex: 1; min-width: 0; }
.hero-left .hero-body { margin-bottom: 0; }
.hero-right {
  display: flex; flex-direction: column; gap: 0.85rem;
  width: 210px; flex-shrink: 0; padding-top: 0.25rem;
}
@media (max-width: 900px) {
  .hero-layout { flex-direction: column; }
  .hero-right { flex-direction: row; width: 100%; }
}
/* summary cards (right of title) */
.summary-card {
  background: var(--g1); border: 1px solid var(--g2);
  border-radius: var(--r-lg); padding: 1.1rem 1.25rem;
}
.sc-pct {
  font-family: var(--ff-head); font-size: 2.8rem; font-weight: 800;
  color: var(--indigo); line-height: 1; margin-bottom: 0.15rem;
}
.sc-label {
  font-family: var(--ff-mono); font-size: 0.58rem; font-weight: 600;
  letter-spacing: 0.1em; text-transform: uppercase; color: var(--g3);
  margin-bottom: 0.3rem;
}
.sc-sub { font-family: var(--ff-mono); font-size: 0.6rem; color: var(--g4); }
.sc-track {
  height: 3px; background: var(--g2); border-radius: 2px;
  overflow: hidden; margin-top: 0.6rem;
}
.sc-fill { height: 100%; border-radius: 2px; }
/* three stat cards (hero bottom row) */
.stat-cards { display: grid; grid-template-columns: repeat(3, 1fr); gap: 1rem; }
@media (max-width: 640px) { .stat-cards { grid-template-columns: 1fr; } }
.stat-card {
  background: var(--g1); border: 1px solid var(--g2);
  border-radius: var(--r-lg); padding: 1.35rem 1.5rem;
}
.scard-type {
  font-family: var(--ff-mono); font-size: 0.6rem; font-weight: 600;
  letter-spacing: 0.1em; text-transform: uppercase; margin-bottom: 0.6rem;
}
.scard-pct {
  font-family: var(--ff-head); font-size: 3rem; font-weight: 800;
  color: var(--indigo); line-height: 1; margin-bottom: 0.6rem;
}
.scard-track { height: 4px; background: var(--g2); border-radius: 2px; overflow: hidden; }
.scard-fill  { height: 100%; border-radius: 2px; }

/* ── Status strip (dark indigo) ── */
.sec-status {
  background: var(--indigo);
  padding: 1.5rem 2rem;
  border-bottom: 3px solid rgba(255,255,255,0.06);
}
.status-inner {
  max-width: var(--mw); margin: 0 auto;
  display: flex; align-items: center; gap: 2rem; flex-wrap: wrap;
}
.status-eyebrow {
  font-family: var(--ff-head); font-size: 2.4rem; font-weight: 800;
  line-height: 1; color: var(--white);
}
.status-divider { width: 1px; height: 36px; background: rgba(255,255,255,0.12); flex-shrink: 0; }
.status-block { display: flex; flex-direction: column; gap: 0.2rem; }
.status-num {
  font-family: var(--ff-head); font-size: 2.4rem; font-weight: 800;
  line-height: 1;
}
.sn-blocked { color: var(--g3); }
.sn-ready   { color: var(--orange); }
.sn-bi      { color: var(--white); }
.status-lbl {
  font-family: var(--ff-mono); font-size: 0.75rem; font-weight: 600;
  letter-spacing: 0.05em; text-transform: uppercase;
  color: rgba(255,255,255,0.65);
}

/* ── Section wrapper ── */
.sec-wrap { max-width: var(--mw); margin: 0 auto; }
.sec-label {
  font-family: var(--ff-mono); font-size: 0.62rem; font-weight: 600;
  letter-spacing: 0.12em; text-transform: uppercase; color: var(--g3);
  margin-bottom: 1.25rem;
}

/* ── Progress columns ── */
.sec-progress {
  background: var(--g1);
  padding: 2.75rem 2rem;
  border-bottom: 1px solid var(--g2);
}
.progress-grid {
  display: grid;
  grid-template-columns: 1fr 1fr 1fr;
  gap: 1.25rem;
  align-items: start;
}
@media (max-width: 860px) { .progress-grid { grid-template-columns: 1fr; } }

.pcol {
  background: var(--white);
  border: 1px solid var(--g2);
  border-radius: var(--r-lg);
  overflow: hidden;
}
.pcol-head {
  padding: 1rem 1.2rem 0.9rem;
  border-bottom: 1px solid var(--g2);
}
.pcol-type {
  font-family: var(--ff-mono); font-size: 0.6rem; font-weight: 600;
  letter-spacing: 0.1em; text-transform: uppercase; margin-bottom: 0.45rem;
}
.pt-dim  { color: #0c6b96; }
.pt-fct  { color: #92400e; }
.pt-cube { color: var(--indigo); }
.pcol-subtitle {
  display: block; font-family: var(--ff-mono); font-size: 0.55rem;
  font-weight: 500; letter-spacing: 0.04em; text-transform: none;
  color: var(--g3); margin-top: 0.15rem;
}
.pcol-score {
  font-family: var(--ff-head); font-size: 1.9rem; font-weight: 800;
  color: var(--indigo); line-height: 1; margin-bottom: 0.55rem;
}
.pcol-score-denom { font-size: 1.1rem; color: var(--g3); }
.prog-track {
  height: 5px; background: var(--g2); border-radius: 3px; overflow: hidden;
}
.prog-fill { height: 100%; border-radius: 3px; }

.item-list { list-style: none; max-height: 400px; overflow-y: auto; padding: 0.4rem 0; }
.item-list::-webkit-scrollbar { width: 4px; }
.item-list::-webkit-scrollbar-track { background: transparent; }
.item-list::-webkit-scrollbar-thumb { background: var(--g2); border-radius: 2px; }

.il-row {
  display: flex; align-items: center; gap: 0.55rem;
  padding: 0.36rem 1.2rem;
  transition: background 0.1s;
}
.il-row:hover { background: var(--g1); }
.il-dot {
  width: 7px; height: 7px; border-radius: 50%; flex-shrink: 0;
}
.il-dot-done    { background: var(--green); }
.il-dot-pending { background: transparent; border: 1.5px solid var(--g3); }
.il-name {
  font-family: var(--ff-mono); font-size: 0.67rem; color: var(--g5);
  flex: 1; min-width: 0; word-break: break-word;
}
.il-row.is-pending .il-name { color: var(--g4); }
.il-aside { display: flex; align-items: center; gap: 0.35rem; flex-shrink: 0; }
.il-badge {
  font-family: var(--ff-mono); font-size: 0.55rem; font-weight: 600;
  border-radius: 3px; padding: 0.1rem 0.4rem; border: 1px solid;
}
.ilb-done    { color: #166534; border-color: rgba(34,197,94,0.4);  background: rgba(34,197,94,0.08);  }
.ilb-pending { color: var(--g4); border-color: var(--g2);          background: transparent;           }
.ilb-blocked { color: #991b1b; border-color: rgba(239,68,68,0.4);  background: rgba(239,68,68,0.07);  }
.ilb-ready   { color: #92400e; border-color: rgba(249,162,26,0.45); background: rgba(249,162,26,0.09); }
.il-meta {
  font-family: var(--ff-mono); font-size: 0.55rem; color: var(--g3);
}

/* ── Cube cards ── */
.sec-cubes {
  background: var(--white);
  padding: 2.75rem 2rem;
  border-bottom: 1px solid var(--g2);
}
.cube-grid {
  display: grid;
  grid-template-columns: repeat(auto-fill, minmax(340px, 1fr));
  gap: 1rem;
}
.cube-card {
  border: 1px solid var(--g2);
  border-radius: var(--r-lg);
  padding: 1.2rem 1.35rem;
  background: var(--white);
  transition: box-shadow 0.15s, border-color 0.15s;
  border-left-width: 3px;
}
.cube-card:hover { box-shadow: 0 4px 20px rgba(0,0,0,0.07); }
.cc-done    { border-left-color: var(--green); }
.cc-blocked { border-left-color: #ef4444; }
.cc-ready   { border-left-color: var(--orange); }

.cc-top {
  display: flex; align-items: flex-start; justify-content: space-between;
  gap: 0.75rem; margin-bottom: 0.8rem;
}
.cc-name {
  font-family: var(--ff-head); font-size: 1.05rem; font-weight: 700;
  color: var(--indigo); line-height: 1.2; word-break: break-word;
}
.cc-status { flex-shrink: 0; margin-top: 0.1rem; }

/* metrics mini-bar */
.cc-metrics {
  margin-bottom: 0.85rem;
}
.cc-metrics-header {
  display: flex; justify-content: space-between; align-items: baseline;
  margin-bottom: 0.3rem;
}
.cc-metrics-label {
  font-family: var(--ff-mono); font-size: 0.58rem; font-weight: 600;
  letter-spacing: 0.08em; text-transform: uppercase; color: var(--g3);
}
.cc-metrics-count {
  font-family: var(--ff-mono); font-size: 0.58rem; color: var(--g4);
}
.cc-track { height: 4px; background: var(--g2); border-radius: 2px; overflow: hidden; }
.cc-fill  { height: 100%; border-radius: 2px; }

/* subtask list inside card */
.cc-sub-list {
  list-style: none; margin-top: 0.5rem;
  display: flex; flex-direction: column; gap: 0.18rem;
  max-height: 140px; overflow-y: auto;
  padding: 0 0 0.25rem;
}
.cc-sub-list::-webkit-scrollbar { width: 3px; }
.cc-sub-list::-webkit-scrollbar-thumb { background: var(--g2); }
.cc-sub-row {
  display: flex; align-items: center; gap: 0.4rem;
  font-family: var(--ff-mono); font-size: 0.62rem;
}
.cc-sub-dot { width: 5px; height: 5px; border-radius: 50%; flex-shrink: 0; }
.cc-sub-done    .cc-sub-dot { background: var(--green); }
.cc-sub-pending .cc-sub-dot { background: transparent; border: 1.5px solid var(--g3); }
.cc-sub-done    .cc-sub-name { color: var(--g3); text-decoration: line-through; }
.cc-sub-pending .cc-sub-name { color: var(--g5); }

/* deps section */
.cc-deps {
  border-top: 1px solid var(--g2);
  padding-top: 0.75rem;
  margin-top: 0.75rem;
}
.cc-deps-label {
  font-family: var(--ff-mono); font-size: 0.58rem; font-weight: 600;
  letter-spacing: 0.08em; text-transform: uppercase; color: var(--g3);
  margin-bottom: 0.4rem;
}
.cc-dep-list {
  list-style: none; display: flex; flex-direction: column; gap: 0.2rem;
}
.cc-dep {
  display: flex; align-items: center; gap: 0.4rem;
  font-family: var(--ff-mono); font-size: 0.62rem;
}
.cc-dep-indicator {
  width: 6px; height: 6px; border-radius: 50%; flex-shrink: 0;
}
.dep-done    .cc-dep-indicator { background: var(--green); }
.dep-pending .cc-dep-indicator { background: transparent; border: 1.5px solid #ef4444; }
.dep-done    .cc-dep-name { color: var(--g3); text-decoration: line-through; }
.dep-pending .cc-dep-name { color: var(--g5); font-weight: 600; }
.cc-dep-type {
  font-size: 0.55rem; color: var(--g3); margin-left: auto; flex-shrink: 0;
}
.cc-no-deps {
  font-family: var(--ff-mono); font-size: 0.62rem; color: var(--g3); font-style: italic;
}

/* ── Filter bar ── */
.cube-toolbar {
  display: flex; align-items: center; justify-content: space-between;
  gap: 1rem; flex-wrap: wrap; margin-bottom: 1.25rem;
}
.filter-bar {
  display: flex; align-items: center; gap: 0.6rem; flex-wrap: wrap;
}
.search-input {
  font-family: var(--ff-mono); font-size: 0.7rem;
  border: 1px solid var(--g2); border-radius: var(--r);
  padding: 0.35rem 0.8rem; outline: none;
  background: var(--g1); color: var(--g5); width: 220px;
  transition: border-color 0.15s, background 0.15s;
}
.search-input:focus { border-color: var(--blue); background: var(--white); }
.search-input::placeholder { color: var(--g3); }
.fchip-group { display: flex; gap: 0.3rem; flex-wrap: wrap; }
.fchip {
  font-family: var(--ff-mono); font-size: 0.6rem; font-weight: 600;
  letter-spacing: 0.04em; border-radius: 4px;
  padding: 0.25rem 0.65rem; border: 1px solid var(--g2);
  background: transparent; color: var(--g4);
  cursor: pointer; transition: all 0.12s; white-space: nowrap;
}
.fchip:hover { border-color: var(--g3); color: var(--g5); background: var(--g1); }
.fchip.fc-active           { background: var(--indigo); color: var(--white); border-color: var(--indigo); }
.fchip.fc-blocked.fc-active { background: #991b1b; border-color: #991b1b; color: var(--white); }
.fchip.fc-ready.fc-active   { background: #92400e; border-color: #92400e; color: var(--white); }
.fchip.fc-done.fc-active    { background: #166534; border-color: #166534; color: var(--white); }
.filter-count {
  font-family: var(--ff-mono); font-size: 0.6rem; color: var(--g3); white-space: nowrap;
}
.filter-clear-btn {
  font-family: var(--ff-mono); font-size: 0.6rem; font-weight: 600;
  border-radius: 4px; padding: 0.25rem 0.65rem;
  border: 1px solid var(--g2); background: transparent; color: var(--g4);
  cursor: pointer; transition: all 0.12s;
}
.filter-clear-btn:hover { background: var(--g2); color: var(--g5); }
/* clickable progress list items */
.il-row-click { cursor: pointer; }
.il-row-click:hover { background: rgba(87,192,233,0.07); }
.il-row-click.il-selected {
  background: rgba(0,30,98,0.07);
  box-shadow: inset 2px 0 0 var(--indigo);
}

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
  letter-spacing: 0.08em; color: rgba(255,255,255,0.35);
}

/* ── Scroll reveal ── */
.rev { opacity: 0; transform: translateY(14px);
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
    <div class="hdr-right" id="hdr-stats"><!-- filled by JS --></div>
  </div>
</header>

<!-- ── HERO ── -->
<section class="hero">
  <div class="hero-inner">
    <div class="hero-layout">
      <div class="hero-left">
        <div class="hero-eyebrow">
          <span class="pulse-dot"></span>
          Semantic Layer Build Progress
        </div>
        <h1 class="hero-title">
          From Dashboards<br>to <span class="hero-title-accent">Metrics</span>
        </h1>
        <p class="hero-body rev d1">
          Tracking migration of Tableau reporting views to dimension and fact models and semantic layer metrics. A metric can only ship once every required model is complete.
        </p>
      </div>
      <div class="hero-right rev d2" id="summary-cards"><!-- filled by JS --></div>
    </div>
  </div>
</section>

<!-- ── STATUS STRIP ── -->
<div class="sec-status">
  <div class="status-inner" id="status-inner"><!-- filled by JS --></div>
</div>

<!-- ── PROGRESS COLUMNS ── -->
<div class="sec-progress">
  <div class="sec-wrap">
    <div class="sec-label rev">Model &amp; Metric Progress</div>
    <div class="progress-grid rev d1">
      <div class="pcol" id="pcol-dims"><!-- filled by JS --></div>
      <div class="pcol" id="pcol-fcts"><!-- filled by JS --></div>
      <div class="pcol" id="pcol-cubes"><!-- filled by JS --></div>
    </div>
  </div>
</div>

<!-- ── CUBE METRIC CARDS ── -->
<div class="sec-cubes">
  <div class="sec-wrap">
    <div class="cube-toolbar rev">
      <div class="sec-label" style="margin-bottom:0">Cube Metric Details &amp; Dependencies</div>
      <div class="filter-bar">
        <input type="text" id="cube-search" class="search-input" placeholder="Search dashboards&#8230;"/>
        <div class="fchip-group" id="filter-chips">
          <button class="fchip fc-active" data-status="all">All</button>
          <button class="fchip fc-blocked" data-status="blocked">&#9650; Blocked</button>
          <button class="fchip fc-ready" data-status="ready">Ready</button>
          <button class="fchip fc-done" data-status="done">Done</button>
        </div>
        <span class="filter-count" id="filter-count"></span>
        <button class="filter-clear-btn" id="filter-clear" style="display:none">Clear</button>
      </div>
    </div>
    <div class="cube-grid rev d1" id="cube-grid"><!-- filled by JS --></div>
  </div>
</div>

<!-- ── FOOTER ── -->
<footer>
  <div class="footer-inner">
    <span class="footer-txt">Data Marts &amp; Semantic Layer &mdash; KTAF</span>
    <span class="footer-txt">Last updated: __UPDATED__</span>
  </div>
</footer>

<script>
const ELEMENTS    = __ELEMENTS__;
const STATS       = __STATS__;
const LAST_UPDATED = "__UPDATED__";

// ── Parse ──────────────────────────────────────────────────────────────────
const nodes   = ELEMENTS.filter(e => e.data && !e.data.source);
const edges   = ELEMENTS.filter(e => e.data && e.data.source);
const nodeMap = Object.fromEntries(nodes.map(n => [n.data.id, n.data]));

const dims  = nodes.filter(n => n.data.type === 'dim') .sort((a,b) => a.data.id.localeCompare(b.data.id));
const fcts  = nodes.filter(n => n.data.type === 'fct') .sort((a,b) => a.data.id.localeCompare(b.data.id));
const cubes = nodes.filter(n => n.data.type === 'cube').sort((a,b) => a.data.id.localeCompare(b.data.id));

// Build cube → deps lookup
const cubeDeps = {};
cubes.forEach(c => { cubeDeps[c.data.id] = []; });
edges.forEach(e => {
  const { source, target } = e.data;
  if (cubeDeps[target] !== undefined) {
    const dep = nodeMap[source];
    if (dep) cubeDeps[target].push(dep);
  }
});

// ── Helpers ────────────────────────────────────────────────────────────────
function pct(done, total) { return total ? Math.round(done / total * 100) : 0; }
function stripCube(name)  { return name.replace(/^cube:\s*/i, ''); }

// ── Sort lists ─────────────────────────────────────────────────────────────
// dims / facts: done first, then pending (alpha within each group)
function doneFirst(a, b) {
  const ak = a.data.status === 'complete' ? 0 : 1;
  const bk = b.data.status === 'complete' ? 0 : 1;
  return ak !== bk ? ak - bk : a.data.id.localeCompare(b.data.id);
}
dims.sort(doneFirst);
fcts.sort(doneFirst);
// cubes: done → ready (deps satisfied) → blocked
function cubeOrder(c) {
  if (c.data.status === 'complete') return 0;
  const deps = cubeDeps[c.data.id] || [];
  return deps.every(d => d.status === 'complete') ? 1 : 2;
}
cubes.sort((a, b) => {
  const ak = cubeOrder(a), bk = cubeOrder(b);
  return ak !== bk ? ak - bk : stripCube(a.data.id).localeCompare(stripCube(b.data.id));
});
function progBar(done, total, color) {
  return `<div class="prog-track"><div class="prog-fill" style="width:${pct(done,total)}%;background:${color}"></div></div>`;
}


// ── Status strip ───────────────────────────────────────────────────────────
const doneCubes    = cubes.filter(c => c.data.status === 'complete');
const readyBICount = cubes.reduce((sum, c) => sum + (c.data.sub_done || 0), 0);
const totalMetrics  = cubes.reduce((sum, c) => sum + (c.data.sub_total || 0), 0);
const blockedCubes = cubes.filter(c => {
  const deps = cubeDeps[c.data.id] || [];
  return c.data.status !== 'complete' && deps.some(d => d.status !== 'complete');
});
const readyCubes   = cubes.filter(c => {
  const deps = cubeDeps[c.data.id] || [];
  return c.data.status !== 'complete' && deps.every(d => d.status === 'complete');
});
// ── Status strip — counts by individual metric subtask ─────────────────────
// A metric is blocked if its parent cube has any pending dep;
// ready for Cube if parent cube deps are all satisfied (cube not yet complete);
// ready for BI if the subtask itself is done.
let metricsBlocked = 0, metricsReadyCube = 0;
cubes.forEach(c => {
  const d    = c.data;
  if (d.status === 'complete') return;
  const deps = cubeDeps[d.id] || [];
  const isBlocked = deps.some(dep => dep.status !== 'complete');
  const pending   = (d.sub_total || 0) - (d.sub_done || 0);
  if (isBlocked) { metricsBlocked   += pending; }
  else           { metricsReadyCube += pending; }
});

document.getElementById('status-inner').innerHTML = `
  <span class="status-eyebrow">Metrics Progress</span>
  <div class="status-divider"></div>
  <div class="status-block">
    <span class="status-num sn-blocked">${metricsBlocked}</span>
    <span class="status-lbl">Blocked</span>
  </div>
  <div class="status-divider"></div>
  <div class="status-block">
    <span class="status-num sn-ready">${metricsReadyCube}</span>
    <span class="status-lbl">Ready for Cube</span>
  </div>
  <div class="status-divider"></div>
  <div class="status-block">
    <span class="status-num sn-bi">${readyBICount}</span>
    <span class="status-lbl">Ready for BI</span>
  </div>
`;

// ── Summary cards (hero right) ─────────────────────────────────────────────
const modelsDone  = STATS.dims_done + STATS.fcts_done;
const modelsTotal = STATS.dims_total + STATS.fcts_total;
const modelsPct   = pct(modelsDone, modelsTotal);
const metricsPct  = pct(readyBICount, totalMetrics);
document.getElementById('summary-cards').innerHTML = `
  <div class="summary-card">
    <div class="sc-pct">${modelsPct}%</div>
    <div class="sc-label">Models</div>
    <div class="sc-sub">${modelsDone}/${modelsTotal} tasks complete</div>
    <div class="sc-track"><div class="sc-fill" style="width:${modelsPct}%;background:#57C0E9"></div></div>
  </div>
  <div class="summary-card">
    <div class="sc-pct">${metricsPct}%</div>
    <div class="sc-label">Metrics</div>
    <div class="sc-sub">${readyBICount}/${totalMetrics} ready for BI</div>
    <div class="sc-track"><div class="sc-fill" style="width:${metricsPct}%;background:#8b5cf6"></div></div>
  </div>
`;

// ── Progress columns ───────────────────────────────────────────────────────
function renderPcol(id, typeLabel, typeClass, color, items, getAside, subtitle, getBadge) {
  const done  = items.filter(n => n.data.status === 'complete').length;
  const total = items.length;
  const rows  = items.map(n => {
    const d     = n.data;
    const ok    = d.status === 'complete';
    const aside = getAside ? getAside(d) : '';
    const badge = getBadge ? getBadge(d)
      : `<span class="il-badge ${ok ? 'ilb-done' : 'ilb-pending'}">${ok ? 'done' : 'pending'}</span>`;
    return `<li class="il-row ${ok ? '' : 'is-pending'}">
      <span class="il-dot ${ok ? 'il-dot-done' : 'il-dot-pending'}"></span>
      <span class="il-name" title="${d.id}">${stripCube(d.id)}</span>
      <span class="il-aside">
        ${aside}
        ${badge}
      </span>
    </li>`;
  }).join('');
  document.getElementById(id).innerHTML = `
    <div class="pcol-head">
      <div class="pcol-type ${typeClass}">${typeLabel}${subtitle ? `<span class="pcol-subtitle">${subtitle}</span>` : ''}</div>
      <div class="pcol-score">${done}<span class="pcol-score-denom">/${total}</span></div>
      ${progBar(done, total, color)}
    </div>
    <ul class="item-list">${rows}</ul>
  `;
}

renderPcol('pcol-dims', 'Dimensions', 'pt-dim', '#57C0E9', dims, null);
renderPcol('pcol-fcts', 'Facts',      'pt-fct', '#F9A21A', fcts, null);
renderPcol('pcol-cubes', 'Tableau Dashboards', 'pt-cube', '#b45309', cubes, d => {
  return d.sub_total > 0
    ? `<span class="il-meta">${d.sub_done}/${d.sub_total}</span>` : '';
}, 'click to filter below', d => {
  if (d.status === 'complete') return `<span class="il-badge ilb-done">done</span>`;
  const deps = cubeDeps[d.id] || [];
  return deps.some(dep => dep.status !== 'complete')
    ? `<span class="il-badge ilb-blocked">blocked</span>`
    : `<span class="il-badge ilb-ready">ready</span>`;
});

// ── Cube detail cards ──────────────────────────────────────────────────────
const cubeCardsHtml = cubes.map(c => {
  const d        = c.data;
  const deps     = cubeDeps[d.id] || [];
  const pendDeps = deps.filter(dep => dep.status !== 'complete');
  const doneDeps = deps.filter(dep => dep.status === 'complete');

  // Card state
  let cardCls, statusHtml, stateKey;
  if (d.status === 'complete') {
    stateKey   = 'done';
    cardCls    = 'cube-card cc-done';
    statusHtml = '<span class="chip c-green">&#10003; complete</span>';
  } else if (pendDeps.length > 0) {
    stateKey   = 'blocked';
    cardCls    = 'cube-card cc-blocked';
    statusHtml = `<span class="chip c-red">&#9650; ${pendDeps.length} blocked</span>`;
  } else {
    stateKey   = 'ready';
    cardCls    = 'cube-card cc-ready';
    statusHtml = '<span class="chip c-gold">ready</span>';
  }
  const safeId = d.id.replace(/"/g, '&quot;');

  // Subtask metrics
  let metricsHtml = '';
  if (d.sub_total > 0) {
    const mp = pct(d.sub_done, d.sub_total);
    const subRows = d.subtasks.map(s => `
      <li class="cc-sub-row ${s.done ? 'cc-sub-done' : 'cc-sub-pending'}">
        <span class="cc-sub-dot"></span>
        <span class="cc-sub-name">${s.name}</span>
      </li>`).join('');
    metricsHtml = `
      <div class="cc-metrics">
        <div class="cc-metrics-header">
          <span class="cc-metrics-label">Metrics</span>
          <span class="cc-metrics-count">${d.sub_done}/${d.sub_total} done (${mp}%)</span>
        </div>
        <div class="cc-track"><div class="cc-fill" style="width:${mp}%;background:#b45309"></div></div>
        <ul class="cc-sub-list">${subRows}</ul>
      </div>`;
  }

  // Dependencies
  let depsHtml;
  if (deps.length > 0) {
    const sorted   = [...pendDeps, ...doneDeps];
    const depLabel = pendDeps.length > 0
      ? `Required models — ${pendDeps.length} still pending`
      : `Required models — all complete`;
    const depRows  = sorted.map(dep => {
      const ok = dep.status === 'complete';
      return `<li class="cc-dep ${ok ? 'dep-done' : 'dep-pending'}">
        <span class="cc-dep-indicator"></span>
        <span class="cc-dep-name">${dep.id}</span>
        <span class="cc-dep-type">${dep.type}</span>
      </li>`;
    }).join('');
    depsHtml = `<div class="cc-deps">
      <div class="cc-deps-label">${depLabel}</div>
      <ul class="cc-dep-list">${depRows}</ul>
    </div>`;
  } else {
    depsHtml = `<div class="cc-deps"><p class="cc-no-deps">No model dependencies tracked in Asana.</p></div>`;
  }

  return `<div class="${cardCls}" data-name="${safeId}" data-state="${stateKey}">
    <div class="cc-top">
      <div class="cc-name">${stripCube(d.id)}</div>
      <div class="cc-status">${statusHtml}</div>
    </div>
    ${metricsHtml}
    ${depsHtml}
  </div>`;
}).join('');

document.getElementById('cube-grid').innerHTML = cubeCardsHtml
  || '<p style="color:var(--g3);font-family:var(--ff-mono);font-size:0.75rem">No cube metrics found.</p>';

// ── Filter & multiselect ───────────────────────────────────────────────────
const selectedIds = new Set();

function applyFilter() {
  const query  = document.getElementById('cube-search').value.toLowerCase().trim();
  const status = document.querySelector('#filter-chips .fchip.fc-active').dataset.status;
  const total  = cubes.length;
  let shown    = 0;

  document.querySelectorAll('#cube-grid .cube-card').forEach(card => {
    const name  = card.dataset.name;
    const state = card.dataset.state;
    let visible;
    if (selectedIds.size > 0) {
      visible = selectedIds.has(name);
    } else {
      const matchQ = !query || name.toLowerCase().includes(query);
      const matchS = status === 'all' || state === status;
      visible = matchQ && matchS;
    }
    card.style.display = visible ? '' : 'none';
    if (visible) shown++;
  });

  document.getElementById('filter-count').textContent =
    shown < total ? `${shown} of ${total}` : `${total} total`;

  const hasClear = selectedIds.size > 0 || query || status !== 'all';
  document.getElementById('filter-clear').style.display = hasClear ? '' : 'none';
}

// Status chip buttons
document.querySelectorAll('#filter-chips .fchip').forEach(btn => {
  btn.addEventListener('click', () => {
    document.querySelectorAll('#filter-chips .fchip').forEach(b => b.classList.remove('fc-active'));
    btn.classList.add('fc-active');
    // Clicking a status filter clears list-click selection
    selectedIds.clear();
    document.querySelectorAll('#pcol-cubes .il-row-click').forEach(r => r.classList.remove('il-selected'));
    applyFilter();
  });
});

// Search input
document.getElementById('cube-search').addEventListener('input', () => {
  selectedIds.clear();
  document.querySelectorAll('#pcol-cubes .il-row-click').forEach(r => r.classList.remove('il-selected'));
  applyFilter();
});

// Clear button
document.getElementById('filter-clear').addEventListener('click', () => {
  selectedIds.clear();
  document.getElementById('cube-search').value = '';
  document.querySelectorAll('#pcol-cubes .il-row-click').forEach(r => r.classList.remove('il-selected'));
  document.querySelectorAll('#filter-chips .fchip').forEach(b => b.classList.remove('fc-active'));
  document.querySelector('#filter-chips .fchip[data-status="all"]').classList.add('fc-active');
  applyFilter();
});

// Click-to-filter on cube progress list items
document.querySelectorAll('#pcol-cubes .il-row').forEach(row => {
  const name = row.querySelector('.il-name')?.getAttribute('title');
  if (!name) return;
  row.classList.add('il-row-click');
  row.title = 'Click to filter';
  row.addEventListener('click', () => {
    if (selectedIds.has(name)) {
      selectedIds.delete(name);
      row.classList.remove('il-selected');
    } else {
      selectedIds.add(name);
      row.classList.add('il-selected');
      // Reset status chips to "All" so selection takes precedence
      document.querySelectorAll('#filter-chips .fchip').forEach(b => b.classList.remove('fc-active'));
      document.querySelector('#filter-chips .fchip[data-status="all"]').classList.add('fc-active');
    }
    applyFilter();
    if (selectedIds.size === 1) {
      document.querySelector('.sec-cubes').scrollIntoView({ behavior: 'smooth', block: 'start' });
    }
  });
});

// Init count
applyFilter();

// ── Scroll reveal ──────────────────────────────────────────────────────────
const revObs = new IntersectionObserver(entries => {
  entries.forEach(e => { if (e.isIntersecting) e.target.classList.add('on'); });
}, { threshold: 0.05 });
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
