import importlib.util
import sys
from pathlib import Path

SCRIPT = Path(__file__).resolve().parents[1] / "generate_dashboard.py"
spec = importlib.util.spec_from_file_location("generate_dashboard", SCRIPT)
gd = importlib.util.module_from_spec(spec)
sys.modules["generate_dashboard"] = gd
spec.loader.exec_module(gd)

D = "1111111111111111"
E = "2222222222222222"


def tag(*names):
    return [{"name": n} for n in names]


def task(gid, name, tags=(), notes="", subtasks=()):
    return {"gid": gid, "name": name, "tags": tag(*tags), "notes": notes,
            "subtasks": list(subtasks), "section": "1 · Assessments"}


def link(gid):
    return f"https://app.asana.com/0/0/{gid}/f"


def test_row_under_pr_task_counts_for_its_dashboard():
    row = task("r1", "national_percentile", ["measure"])
    pr = task("p1", "PR L1 (Cube): percentile", subtasks=[row])
    dash = task(D, "Literacy Dashboard", ["dashboard"], subtasks=[pr])
    assert gd.linked_rows([dash])[D]["measures"] == [row]


def test_dimension_row_under_pr_needs_its_tag():
    tagged = task("r1", "quiz number", ["dimension"])
    untagged = task("r2", "value group")
    pr = task("p1", "PR SW2 (Cube): sight words", subtasks=[tagged, untagged])
    dash = task(D, "Literacy Dashboard", ["dashboard"], subtasks=[pr])
    assert gd.linked_rows([dash])[D]["dimensions"] == [tagged]


def test_row_under_own_dashboard_counts_once():
    row = task("r1", "pct_proficient", ["measure"], notes=f"Used by: {link(D)} {link(E)}")
    holder = task("h1", "Already in Cube", subtasks=[row])
    dash = task(D, "Literacy Dashboard", ["dashboard"], subtasks=[holder])
    other = task(E, "APM Dashboard", ["dashboard"])
    linked = gd.linked_rows([dash, other])
    assert linked[D]["measures"] == [row]
    assert linked[E]["measures"] == [row]


def test_table_rows_still_read():
    row = task("r1", "Sight word", notes=f"Used by: {link(D)}")
    table = task("t1", "fct_sight_words", subtasks=[row])
    dash = task(D, "Literacy Dashboard", ["dashboard"])
    assert gd.linked_rows([table, dash])[D]["dimensions"] == [row]


def test_build_dashboard_does_not_repeat_direct_measures():
    direct = task("m1", "% Proficient", ["measure"])
    dash = task(D, "Literacy Dashboard", ["dashboard"], subtasks=[direct])
    out = gd.build_dashboard(dash, gd.linked_rows([dash]).get(D, {"measures": [], "dimensions": []}), [])
    assert [m["name"] for m in out["measures"]] == ["% Proficient"]
