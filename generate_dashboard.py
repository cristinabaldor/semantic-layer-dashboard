"""
generate_dashboard.py
─────────────────────────────────────────────────────────────────────────────
Reads the "Data Marts + Semantic Layer" Asana project (read-only) and writes
data.js for the semantic layer build status page on the teamster docs site
(docs/launch/semantic-layer/, which loads this repo's Pages copy): one record per
Tableau dashboard, plus milestones.

What it reads
  Dashboard tasks   tag `dashboard`; section = domain ("1 · …").
    checklist       subtasks named like CHECKLIST below (Anthony's transition steps).
    measure rows    subtasks tagged `measure`, from two places:
                      - directly under the dashboard task; a `Used by:` line in
                        its notes adds the other dashboards it serves;
                      - under a table task (`dim_*`, `fct_*`, `bridge_*`) whose
                        notes' `Used by:` line links the dashboard task.
                    One row serves every dashboard it links.
                    Done = live in Cube; open rows carry at most one of STATUS_TAGS.
    dimensions      dimension rows: other subtasks of table tasks whose notes'
                    `Used by:` line links the dashboard task, read like measure
                    rows; how each is used is its notes' `Use:` line.
  Milestones        top-level tasks named `M0`, `M1.1` … `M5.5`.
teamster's scripts/asana_sync.py sets the ticks and tags this script reads.

Only names, counts, statuses, dates and each dimension's `Use:` line are
written. Nothing else from the notes leaves this script; the output is
published on a public page.

USAGE
  export ASANA_PAT=…        (or put ASANA_PAT=… in .env)
  python3 generate_dashboard.py
"""

import datetime
import json
import os
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

# ── CONFIG ────────────────────────────────────────────────────────────────────
PROJECT_GID = "1213735218595734"
OUTPUT_FILE = "data.js"
API = "https://app.asana.com/api/1.0"
TASK_FIELDS = ("name,completed,tags.name,due_on,start_on,notes,"
               "num_subtasks,resource_subtype")

CHECKLIST = ["Cube work complete", "Matches within tolerance", "Access verified",
             "Pilot run", "Launched"]
# Earlier Asana names for a checklist step, read as the current name.
OLD_STEP_NAMES = {"Built in Cube": "Cube work complete"}
# "Fully available" (Cube can answer everything the dashboard shows) needs these
# steps ticked; Pilot run and Launched are rollout after it.
AVAILABLE_STEPS = CHECKLIST[:3]
# Dashboards in these sections are listed apart from this year's totals.
OUTSIDE_PLAN_PREFIXES = ("2027-28", "2026-27 Stretch")

# Status tag → page status key, for measure and dimension rows. A done row is always "ready".
STATUS_TAGS = {
    "needs-cube": "cube_work",
    "needs-data": "data_work",
    # rows still carry these until asana_sync.py's next --apply strips them
    "cube-covered": "cube_work",
    "cube-partial": "cube_work",
    "mart-ready":   "cube_work",
    "mart-missing": "data_work",
}
USE_RE = re.compile(r"^\s*Use:\s*(.+)$", re.M)
USED_BY_RE = re.compile(r"^\s*Used by:\s*(.*)$", re.M)  # Asana task links; each carries its gid
TABLE_RE = re.compile(r"^(dim|fct|bridge)_[a-z0-9_]+$")
MILESTONE_RE = re.compile(r"^(M\d+(?:\.\d+)?)\s+(.*)$")


# ── ASANA (read-only GETs) ────────────────────────────────────────────────────

def _get(pat, path, **params):
    """GET every page of an Asana collection."""
    params = {**params, "limit": 100}
    out = []
    while True:
        url = f"{API}{path}?{urllib.parse.urlencode(params)}"
        req = urllib.request.Request(url, headers={"Authorization": f"Bearer {pat}"})
        for attempt in range(5):
            try:
                with urllib.request.urlopen(req, timeout=60) as r:
                    body = json.load(r)
                break
            except urllib.error.HTTPError as e:
                if e.code == 429 or e.code >= 500:
                    wait = int(e.headers.get("Retry-After", 2 ** attempt))
                    print(f"  ⏳ Asana {e.code}, waiting {wait}s…", file=sys.stderr)
                    time.sleep(wait)
                    continue
                raise
        else:
            raise RuntimeError(f"Asana kept failing for {path}")
        out += body["data"]
        if not body.get("next_page"):
            return out
        params["offset"] = body["next_page"]["offset"]


def fetch_project(pat):
    """Top-level tasks with their section name and direct subtasks."""
    tasks = []
    sections = _get(pat, f"/projects/{PROJECT_GID}/sections", opt_fields="name")
    for sec in sections:
        for t in _get(pat, f"/sections/{sec['gid']}/tasks", opt_fields=TASK_FIELDS):
            t["section"] = sec["name"]
            t["subtasks"] = (_get(pat, f"/tasks/{t['gid']}/subtasks", opt_fields=TASK_FIELDS)
                             if t.get("num_subtasks") else [])
            # a table task nested under another task holds measure and dimension rows too
            for sub in t["subtasks"]:
                if TABLE_RE.match(sub["name"].strip()) and sub.get("num_subtasks"):
                    sub["subtasks"] = _get(pat, f"/tasks/{sub['gid']}/subtasks",
                                           opt_fields=TASK_FIELDS)
            tasks.append(t)
    print(f"  {len(sections)} sections, {len(tasks)} tasks, "
          f"{sum(len(t['subtasks']) for t in tasks)} subtasks.")
    return tasks


def linked_rows(tasks):
    """Dashboard gid → {"measures": [...], "dimensions": [...]}: rows whose `Used by:`
    links it. Table-task subtasks are measure rows when tagged `measure`, else
    dimension rows. A measure directly under a dashboard links the other dashboards
    its `Used by:` names; its own dashboard already counts it."""
    tables = [t for t in tasks if TABLE_RE.match(t["name"].strip())]
    tables += [s for t in tasks for s in t["subtasks"] if TABLE_RE.match(s["name"].strip())]
    sources = [(None, row) for table in tables for row in table.get("subtasks", [])]
    sources += [(t["gid"], row) for t in tasks if "dashboard" in _tags(t)
                for row in t["subtasks"] if "measure" in _tags(row)]
    out = {}
    for parent_gid, row in sources:
        used_by = USED_BY_RE.search(row.get("notes") or "")
        kind = "measures" if "measure" in _tags(row) else "dimensions"
        for gid in set(re.findall(r"\b(\d{16})\b", used_by.group(1))) if used_by else ():
            if gid != parent_gid:
                out.setdefault(gid, {"measures": [], "dimensions": []})[kind].append(row)
    return out


# ── PARSING ───────────────────────────────────────────────────────────────────

def _tags(task):
    return {t["name"].lower() for t in task.get("tags", [])}


def _domain(section_name):
    """'1 · Ops: attendance' → 'Ops: attendance'."""
    return re.sub(r"^\d+\s*·\s*", "", section_name).strip()


def row_status(sub):
    if sub.get("completed"):
        return "ready"
    for tag in _tags(sub):
        if tag in STATUS_TAGS:
            return STATUS_TAGS[tag]
    return "unset"


def _measure(sub):
    return {"name":   sub["name"].strip()[:200],
            "status": row_status(sub),
            "due":    sub.get("due_on") or ""}


def build_dashboard(task, linked, warnings):
    measures = [_measure(m) for m in linked["measures"]]
    checklist = {}
    dimensions = []
    for d in linked["dimensions"]:
        use = USE_RE.search(d.get("notes") or "")
        dimensions.append({"name":   d["name"].strip()[:120],
                           "use":    use.group(1).strip()[:120] if use else "",
                           "status": row_status(d)})
    for sub in task["subtasks"]:
        step = OLD_STEP_NAMES.get(sub["name"].strip(), sub["name"].strip())
        if step in CHECKLIST:
            checklist[step] = {"done": bool(sub.get("completed")),
                               "due": sub.get("due_on") or ""}
        elif "measure" in _tags(sub):
            measures.append(_measure(sub))
        else:
            warnings.append(f"'{task['name'].strip()}': untagged subtask "
                            f"'{sub['name'][:60]}' skipped")

    if not dimensions:
        warnings.append(f"'{task['name'].strip()}': no dimension row's `Used by:` names it")

    # Stage = furthest checklist step ticked (0 = no checklist or nothing ticked).
    stage = max((i + 1 for i, step in enumerate(CHECKLIST)
                 if checklist.get(step, {}).get("done")), default=0)
    launched = checklist.get("Launched", {}).get("done", False)
    rows = measures + dimensions
    available = (bool(rows) and all(r["status"] == "ready" for r in rows)
                 and all(checklist.get(s, {}).get("done") for s in AVAILABLE_STEPS))

    return {
        "gid":           task["gid"],
        "name":          task["name"].strip(),
        "domain":        _domain(task["section"]),
        "due":           task.get("due_on") or checklist.get("Launched", {}).get("due", ""),
        "measures":      measures,
        "dimensions":    dimensions or [],
        "has_checklist": bool(checklist),
        "checklist":     [checklist.get(s, {}).get("done", False) for s in CHECKLIST],
        "stage":         stage,
        "launched":      launched,
        "available":     available,
        # Same value under the old name, for a page that predates "available".
        "transitioned":  available,
    }


def build_data(tasks):
    warnings = []
    dashboards, parking_lot, milestones = [], [], []
    linked = linked_rows(tasks)
    empty = {"measures": [], "dimensions": []}

    for t in tasks:
        tags = _tags(t)
        if "dashboard" in tags:
            if not t.get("notes", "").strip() and not t["subtasks"]:
                warnings.append(f"'{t['name'].strip()}' ({t['section']}): empty dashboard "
                                "task, left off the page")
                continue
            d = build_dashboard(t, linked.get(t["gid"], empty), warnings)
            (parking_lot if t["section"].startswith(OUTSIDE_PLAN_PREFIXES)
             else dashboards).append(d)
        elif "measure" in tags:
            warnings.append(f"measure '{t['name'].strip()}' is not under a dashboard, "
                            "so it is not counted")
        m = MILESTONE_RE.match(t["name"].strip())
        if m:
            milestones.append({
                "code":       m.group(1),
                "name":       m.group(2).strip(),
                "workstream": _domain(t["section"]),
                "done":       bool(t.get("completed")),
                "due":        t.get("due_on") or "",
                "start":      t.get("start_on") or "",
            })

    def code_key(ms):
        return [int(x) for x in ms["code"][1:].split(".")]
    milestones.sort(key=code_key)
    return {"dashboards": dashboards, "parking_lot": parking_lot,
            "milestones": milestones, "checklist_steps": CHECKLIST,
            "available_steps": len(AVAILABLE_STEPS)}, warnings


# ── MAIN ──────────────────────────────────────────────────────────────────────

def _load_dotenv():
    if os.path.exists(".env"):
        for line in open(".env", encoding="utf-8"):
            key, sep, value = line.strip().partition("=")
            if sep and key and not key.startswith("#"):
                os.environ.setdefault(key, value.strip().strip("'\""))


def main():
    _load_dotenv()
    pat = os.environ.get("ASANA_PAT")
    if not pat:
        sys.exit("Error: ASANA_PAT is not set (export it or put it in .env).")

    print("Reading Asana…")
    data, warnings = build_data(fetch_project(pat))
    data["updated"] = datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="minutes")

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        f.write("const DATA = " + json.dumps(data, ensure_ascii=False, separators=(",", ":"))
                + ";\n")

    ds = data["dashboards"]
    print(f"  Dashboards: {len(ds)} core ({sum(d['available'] for d in ds)} fully available), "
          f"{len(data['parking_lot'])} outside this year's plan")
    print(f"  Metrics ready: {sum(m['status'] == 'ready' for d in ds for m in d['measures'])}"
          f"/{sum(len(d['measures']) for d in ds)}")
    print(f"  Milestones: {len(data['milestones'])} "
          f"({sum(1 for m in data['milestones'] if m['due'])} dated)")
    for w in warnings:
        print(f"  ⚠ {w}")
    print(f"✓ Written to {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
