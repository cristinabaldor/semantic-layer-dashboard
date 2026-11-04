"""
generate_dashboard.py
─────────────────────────────────────────────────────────────────────────────
Reads the "Data Marts + Semantic Layer" Asana project (read-only) and writes
data.js for the semantic layer build status page on the teamster docs site
(docs/launch/semantic-layer/, which loads this repo's Pages copy): one record per
Tableau dashboard, one per domain, plus milestones.

What it reads
  Domain steps      top-level tasks in a domain's section, matched by name with
                    DOMAIN_STEPS below: "Cubes: <domain> facts", "<domain>
                    verified", "Security review: <domain>", "Pilot run: <domain>",
                    "<domain> launched". Progress is tracked per domain.
  Dashboard tasks   tag `dashboard`; section = domain ("1 · …"). Untagged subtasks
                    (PR plans, issue copies, decisions) are work items, not rows.
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
  Milestones        top-level tasks named `M0`, `M1.1` … `M5.5`, plus each
                    domain's verified and launched tasks (phase 1, no code).
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

# Each domain's steps, in order: page name → pattern for the task's name.
DOMAIN_STEPS = [
    ("Built in Cube",   re.compile(r"^Cubes: .+ facts\b")),
    ("Verified",        re.compile(r" verified$")),
    ("Security review", re.compile(r"^Security review: ")),
    ("Pilot run",       re.compile(r"^Pilot run: ")),
    ("Launched",        re.compile(r" launched$")),
]
# Subtasks of a domain's "Pilot run" task, earliest first; the page shows the
# furthest one ticked. Any other ticked subtask ranks after these.
PILOT_STATUS_ORDER = ("Domain Owner Selected", "Pilot Group Identified")
# Domain steps that are milestones: listed on the timeline too.
MILESTONE_STEPS = ("Verified", "Launched")
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
# A facts task that is also its table's task names the table in parentheses:
# "Cubes: Zendesk facts (fct_support_tickets)".
TABLE_IN_NAME_RE = re.compile(r"\((dim|fct|bridge)_[a-z0-9_]+\)$")
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
                if _is_table(sub) and sub.get("num_subtasks"):
                    sub["subtasks"] = _get(pat, f"/tasks/{sub['gid']}/subtasks",
                                           opt_fields=TASK_FIELDS)
            tasks.append(t)
    print(f"  {len(sections)} sections, {len(tasks)} tasks, "
          f"{sum(len(t['subtasks']) for t in tasks)} subtasks.")
    return tasks


def _is_table(task):
    name = task["name"].strip()
    return bool(TABLE_RE.match(name) or TABLE_IN_NAME_RE.search(name))


def linked_rows(tasks):
    """Dashboard gid → {"measures": [...], "dimensions": [...]}: rows whose `Used by:`
    links it. Table-task subtasks are measure rows when tagged `measure`, else
    dimension rows. A measure directly under a dashboard links the other dashboards
    its `Used by:` names; its own dashboard already counts it."""
    tables = [t for t in tasks if _is_table(t)]
    tables += [s for t in tasks for s in t["subtasks"] if _is_table(s)]
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
    dimensions = []
    for d in linked["dimensions"]:
        use = USE_RE.search(d.get("notes") or "")
        dimensions.append({"name":   d["name"].strip()[:120],
                           "use":    use.group(1).strip()[:120] if use else "",
                           "status": row_status(d)})
    # A dashboard also holds its work items (PR plans, issue copies, decisions);
    # only its `measure`-tagged subtasks are rows.
    for sub in task["subtasks"]:
        if "measure" in _tags(sub):
            measures.append(_measure(sub))

    if not dimensions:
        warnings.append(f"'{task['name'].strip()}': no dimension row's `Used by:` names it")

    return {
        "gid":        task["gid"],
        "name":       task["name"].strip(),
        "domain":     _domain(task["section"]),
        "due":        task.get("due_on") or "",
        "measures":   measures,
        "dimensions": dimensions,
    }


def build_domains(tasks, dashboards, warnings):
    """One record per domain with a dashboard this year, in section order:
    each DOMAIN_STEPS task's tick and due date. Stage = furthest step ticked.
    pilot_status = the name of the Pilot run task's furthest ticked subtask in
    PILOT_STATUS_ORDER; "" when none is ticked."""
    domains = []
    for name in dict.fromkeys(d["domain"] for d in dashboards):
        in_section = [t for t in tasks if _domain(t["section"]) == name]
        steps, pilot = [], {}
        for step, pattern in DOMAIN_STEPS:
            hits = [t for t in in_section if pattern.search(t["name"].strip())]
            if len(hits) != 1:
                warnings.append(f"domain '{name}': {len(hits)} '{step}' tasks, expected 1")
            t = hits[0] if hits else {}
            if step == "Pilot run":
                pilot = t
            steps.append({"name": t.get("name", "").strip(),
                          "done": bool(t.get("completed")),
                          "due":  t.get("due_on") or ""})
        rank = {n.lower(): i for i, n in enumerate(PILOT_STATUS_ORDER)}
        ticked = sorted((s["name"].strip() for s in pilot.get("subtasks", [])
                         if s.get("completed")),
                        key=lambda n: rank.get(n.lower(), len(rank)))
        domains.append({
            "name":  name,
            "steps": steps,
            "stage": max((i + 1 for i, s in enumerate(steps) if s["done"]), default=0),
            "pilot_status": ticked[-1] if ticked else "",
        })
    return domains


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
                "phase":      int(m.group(1)[1:].split(".")[0]),
                "name":       m.group(2).strip(),
                "workstream": _domain(t["section"]),
                "done":       bool(t.get("completed")),
                "due":        t.get("due_on") or "",
                "start":      t.get("start_on") or "",
            })

    domains = build_domains(tasks, dashboards, warnings)
    for dom in domains:
        for (step, _), s in zip(DOMAIN_STEPS, dom["steps"]):
            if step in MILESTONE_STEPS and s["name"]:
                milestones.append({"code": "", "phase": 1, "name": s["name"],
                                   "workstream": dom["name"], "done": s["done"],
                                   "due": s["due"], "start": ""})

    def ms_key(ms):
        code = [int(x) for x in ms["code"][1:].split(".")] if ms["code"] else [99]
        return (ms["phase"], code, ms["due"] or "9999", ms["name"])
    milestones.sort(key=ms_key)
    return {"dashboards": dashboards, "parking_lot": parking_lot, "domains": domains,
            "milestones": milestones,
            "domain_steps": [step for step, _ in DOMAIN_STEPS]}, warnings


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
    doms = data["domains"]
    print(f"  Domains: {len(doms)} ({sum(d['steps'][1]['done'] for d in doms)} verified, "
          f"{sum(d['steps'][-1]['done'] for d in doms)} launched)")
    print(f"  Dashboards: {len(ds)} core, "
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
