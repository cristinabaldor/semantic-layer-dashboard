"""
sync_asana.py
─────────────────────────────────────────────────────────────────────────────
Syncs the Asana project (1213735218595734) with the final built-out marts in
  src/dbt/kipptaf/models/marts/

For each mart model it will:
  • Find the matching Asana task (by name) — or create it if missing
  • Mark the task complete

Sections are matched by keyword (case-insensitive) and created if absent:
  "Dimensions" → dim_* models
  "Facts"      → fct_* models
  "Bridges"    → bridge_* models  (new section; also adds bridge rendering
                                   support via a note — see generate_dashboard.py)

USAGE
─────
  export ASANA_PAT="your-personal-access-token"
  python3 sync_asana.py             # preview (dry-run)
  python3 sync_asana.py --apply     # actually create/update tasks in Asana
"""

import os
import sys
import time
import argparse
import asana
from asana.rest import ApiException


# ── CONFIG ────────────────────────────────────────────────────────────────────
PROJECT_GID = "1213735218595734"

# Full list of final mart models keyed by section keyword → task names
# (names match the .sql filenames without the extension)
EXPECTED = {
    "Dimensions": [
        "dim_assessment_comparisons",
        "dim_assessment_targets",
        "dim_assessments",
        "dim_college_enrollments",
        "dim_colleges",
        "dim_course_sections",
        "dim_courses",
        "dim_dates",
        "dim_job_candidates",
        "dim_job_postings",
        "dim_locations",
        "dim_regions",
        "dim_school_calendars",
        "dim_staff",
        "dim_staff_observation_expectations",
        "dim_staff_observation_microgoal_types",
        "dim_staff_observation_rubric_measurements",
        "dim_staff_observation_rubrics",
        "dim_staff_observation_types",
        "dim_staff_status",
        "dim_staff_work_assignments",
        "dim_staffing_positions",
        "dim_student_assessment_expectations",
        "dim_student_attendance_intervention_types",
        "dim_student_contact_persons",
        "dim_student_enrollments",
        "dim_student_section_enrollments",
        "dim_students",
        "dim_survey_administrations",
        "dim_survey_expectations",
        "dim_survey_questions",
        "dim_surveys",
        "dim_terms",
        "dim_work_assignment_jobs",
        "dim_work_assignment_locations",
        "dim_work_assignment_organizational_units",
        "dim_work_assignment_reporting_relationships",
        "dim_work_assignment_status",
        "dim_work_assignment_types",
    ],
    "Facts": [
        "fct_assessment_scores_enrollment_scoped",
        "fct_assessment_scores_student_scoped",
        "fct_behavioral_consequences",
        "fct_behavioral_incidents",
        "fct_family_communications",
        "fct_grades_assignments",
        "fct_grades_category",
        "fct_grades_gpa",
        "fct_grades_term",
        "fct_job_candidate_applications",
        "fct_staff_attrition",
        "fct_staff_benefits_enrollments",
        "fct_staff_membership_enrollments",
        "fct_staff_observation_microgoals",
        "fct_staff_observation_scores",
        "fct_staff_observations",
        "fct_student_attendance_daily",
        "fct_student_attendance_interventions",
        "fct_student_attendance_streaks",
        "fct_support_tickets",
        "fct_survey_responses",
        "fct_survey_submissions",
        "fct_work_assignment_additional_earnings",
        "fct_work_assignment_compensation",
    ],
    "Bridges": [
        "bridge_course_section_teachers",
        "bridge_course_section_terms",
        "bridge_student_contacts",
        "bridge_survey_questions",
    ],
}

TOTAL_EXPECTED = sum(len(v) for v in EXPECTED.values())


# ── ASANA HELPERS ─────────────────────────────────────────────────────────────

def _make_apis(pat):
    cfg = asana.Configuration()
    cfg.access_token = pat
    client = asana.ApiClient(cfg)
    return (
        asana.SectionsApi(client),
        asana.TasksApi(client),
    )


def _call(fn, *args, **kwargs):
    """Rate-limit-aware Asana call with exponential back-off."""
    for attempt in range(6):
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


# ── FETCH CURRENT STATE ───────────────────────────────────────────────────────

def fetch_current(sections_api, tasks_api):
    """Returns {section_name: {task_name: task_gid, ...}, ...}"""
    raw_sections = list(_call(
        sections_api.get_sections_for_project,
        PROJECT_GID,
        opts={"opt_fields": "gid,name"},
    ))

    state = {}
    for sec in raw_sections:
        raw_tasks = list(_call(
            tasks_api.get_tasks_for_section,
            sec["gid"],
            opts={"opt_fields": "gid,name,completed", "limit": 100},
        ))
        state[sec["name"]] = {
            "_gid": sec["gid"],
            **{t["name"]: {"gid": t["gid"], "completed": t.get("completed", False)}
               for t in raw_tasks},
        }
        time.sleep(0.15)
    return state


# ── SECTION MATCHING ─────────────────────────────────────────────────────────

def _find_section(state, keyword):
    """Return the state key whose name contains keyword (case-insensitive), else None."""
    kw = keyword.lower()
    for name in state:
        if kw in name.lower():
            return name
    return None


# ── MAIN SYNC ─────────────────────────────────────────────────────────────────

def sync(pat, apply: bool):
    sections_api, tasks_api = _make_apis(pat)

    print(f"\nFetching current Asana project state...")
    state = fetch_current(sections_api, tasks_api)
    print(f"  {len(state)} sections, "
          f"{sum(len(v) - 1 for v in state.values())} tasks currently in project.\n")

    created_sections = 0
    created_tasks    = 0
    completed_tasks  = 0
    already_done     = 0

    for section_label, model_names in EXPECTED.items():
        # ── Find or create the Asana section ──────────────────────────────────
        existing_key = _find_section(state, section_label.rstrip("s"))  # "Dimension" matches "Dimensions" etc.
        if existing_key is None:
            print(f"[SECTION] '{section_label}' not found in Asana.", end="")
            if apply:
                sec = _call(
                    sections_api.create_section_for_project,
                    PROJECT_GID,
                    {"data": {"name": section_label}},
                )
                sec_gid = sec["gid"]
                state[section_label] = {"_gid": sec_gid}
                existing_key = section_label
                created_sections += 1
                print(" → created.")
            else:
                print(" (dry-run: would create)")
                state[section_label] = {"_gid": None}
                existing_key = section_label
        else:
            print(f"[SECTION] '{section_label}' → matched '{existing_key}'")

        sec_gid    = state[existing_key]["_gid"]
        sec_tasks  = {k: v for k, v in state[existing_key].items() if k != "_gid"}

        # ── Process each model ────────────────────────────────────────────────
        for model in model_names:
            if model not in sec_tasks:
                # Task doesn't exist → create it (and mark complete)
                print(f"  [CREATE] {model}", end="")
                if apply and sec_gid:
                    task = _call(
                        tasks_api.create_task,
                        {"data": {
                            "name":      model,
                            "projects":  [PROJECT_GID],
                            "memberships": [{"project": PROJECT_GID, "section": sec_gid}],
                            "completed": True,
                        }},
                    )
                    created_tasks += 1
                    completed_tasks += 1
                    print(f" → created & marked complete (gid={task['gid']})")
                    time.sleep(0.2)
                else:
                    print(" (dry-run: would create & complete)")
            else:
                task_info = sec_tasks[model]
                if task_info["completed"]:
                    already_done += 1
                    print(f"  [OK]     {model}  ✓ already complete")
                else:
                    print(f"  [DONE]   {model}", end="")
                    if apply:
                        _call(
                            tasks_api.update_task,
                            task_info["gid"],
                            {"data": {"completed": True}},
                        )
                        completed_tasks += 1
                        print(" → marked complete")
                        time.sleep(0.15)
                    else:
                        print(" (dry-run: would mark complete)")

        print()

    # ── Summary ───────────────────────────────────────────────────────────────
    print("─" * 60)
    print(f"Expected models : {TOTAL_EXPECTED}")
    print(f"Already done    : {already_done}")
    if apply:
        print(f"Sections created: {created_sections}")
        print(f"Tasks created   : {created_tasks}")
        print(f"Tasks completed : {completed_tasks}")
        print("\nDone. Re-run generate_dashboard.py to refresh index.html.")
    else:
        print("\nDry-run complete. Run with --apply to make changes in Asana.")


# ── ENTRYPOINT ────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Sync Asana plan with final mart models.")
    parser.add_argument("--apply", action="store_true",
                        help="Actually create/update tasks (default is dry-run)")
    args = parser.parse_args()

    pat = os.environ.get("ASANA_PAT")
    if not pat:
        print("Error: ASANA_PAT environment variable not set.", file=sys.stderr)
        sys.exit(1)

    sync(pat, apply=args.apply)
