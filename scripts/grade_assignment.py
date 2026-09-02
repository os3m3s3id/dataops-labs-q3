"""
DataOps Mentorship — Automated Grading Script (Intern Track: Q3 & Q4)
=====================================================================
Grades student dbt submissions for Weeks 1–6. Each week builds a slice of a
star-schema data warehouse from the raw CSV seeds.

Usage:
    python scripts/grade_assignment.py --week 1
    ...
    python scripts/grade_assignment.py --week 6
"""

import argparse
import io
import json
import os
import re
import sys

# Fix Unicode output on Windows terminals
if sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
    sys.stdout = io.TextIOWrapper(
        sys.stdout.buffer, encoding="utf-8", errors="replace"
    )

# ── Paths ─────────────────────────────────────────────────────
DBT_PROJECT_DIR = os.path.join(os.path.dirname(__file__), "..", "dbt_learning")
MODELS_DIR = os.path.join(DBT_PROJECT_DIR, "models")
STAGE_DIR = os.path.join(MODELS_DIR, "stage")
DEV_DIR = os.path.join(MODELS_DIR, "dev")
TESTS_DIR = os.path.join(DBT_PROJECT_DIR, "tests")
MACROS_DIR = os.path.join(DBT_PROJECT_DIR, "macros")
RESULTS_PATH = os.path.join(DBT_PROJECT_DIR, "target", "run_results.json")
DBT_PROJECT_YML = os.path.join(DBT_PROJECT_DIR, "dbt_project.yml")
REPO_ROOT = os.path.join(os.path.dirname(__file__), "..")
AIRFLOW_DAGS_DIR = os.path.join(REPO_ROOT, "airflow", "dags")

# The 5 staging models (note: stg_stores, from the raw_store_locations seed).
STAGE_MODELS = ["stg_customers", "stg_products", "stg_orders",
                "stg_order_items", "stg_stores"]


# ═════════════════════════════════════════════════════════════
#  HELPERS
# ═════════════════════════════════════════════════════════════

def file_exists(path):
    """Return a file's content if it exists, else None."""
    full = os.path.normpath(path)
    if os.path.isfile(full):
        with open(full, "r", encoding="utf-8", errors="replace") as f:
            return f.read()
    return None


def check_file_exists(path, label):
    content = file_exists(path)
    if content is not None:
        return True, f"✅ {label} — file found"
    return False, f"❌ {label} — file NOT found"


def check_file_contains(path, pattern, label, case_insensitive=True):
    content = file_exists(path)
    if content is None:
        return False, f"❌ {label} — file not found"
    flags = re.IGNORECASE if case_insensitive else 0
    if re.search(pattern, content, flags):
        return True, f"✅ {label}"
    return False, f"❌ {label}"


def check_text_contains(content, pattern, label, case_insensitive=True):
    """Like check_file_contains but against an in-memory string."""
    flags = re.IGNORECASE if case_insensitive else 0
    if content and re.search(pattern, content, flags):
        return True, f"✅ {label}"
    return False, f"❌ {label}"


def concat_dir_content(directory, exts=(".yml", ".yaml")):
    """Concatenate every file with the given extensions under `directory`."""
    blob = []
    if os.path.isdir(directory):
        for root, _dirs, files in os.walk(directory):
            for fname in files:
                if fname.lower().endswith(exts):
                    c = file_exists(os.path.join(root, fname))
                    if c:
                        blob.append(c)
    return "\n".join(blob)


def load_dbt_results():
    if os.path.isfile(RESULTS_PATH):
        with open(RESULTS_PATH, "r") as f:
            return json.load(f)
    return None


def _find_result(results_data, fragment):
    """Return the result for `fragment`.

    Prefers an exact node-name match (unique_id ending in `.<fragment>`) so that
    e.g. "fct_orders" does not accidentally match "example_fct_orders". Falls
    back to a substring match (needed for tests whose ids carry a hash suffix).
    """
    if results_data is None:
        return None
    rows = results_data.get("results", [])
    for res in rows:
        if res.get("unique_id", "").endswith("." + fragment):
            return res
    for res in rows:
        if fragment in res.get("unique_id", ""):
            return res
    return None


def check_dbt_result(results_data, fragment, label):
    """Check that a model/test with `fragment` in its id passed."""
    if results_data is None:
        return False, f"⏳ {label} — no dbt results (run dbt first)"
    res = _find_result(results_data, fragment)
    if res is None:
        return False, f"⏳ {label} — not found in dbt results"
    status = res.get("status", "")
    if status in ("pass", "success"):
        return True, f"✅ {label}"
    msg = (res.get("message") or "")[:80]
    return False, f"❌ {label} — status: {status} {msg}"


def check_model_rows(results_data, fragment, expected, label):
    """Check that a model built with `expected` rows (falls back to pass-status
    when the adapter did not report a row count)."""
    if results_data is None:
        return False, f"⏳ {label} — no dbt results (run dbt first)"
    res = _find_result(results_data, fragment)
    if res is None:
        return False, f"⏳ {label} — {fragment} not found in dbt results"
    if res.get("status") not in ("pass", "success"):
        return False, f"❌ {label} — model did not build ({res.get('status')})"
    rows = (res.get("adapter_response") or {}).get("rows_affected")
    if rows is None:
        # Row count not reported — accept the successful build.
        return True, f"✅ {label} — built (row count not reported)"
    if int(rows) == expected:
        return True, f"✅ {label} — {rows} rows"
    return False, f"❌ {label} — {rows} rows (expected {expected})"


def strip_py_comments(code):
    """Remove triple-quoted blocks and `#` comments so TODO hints in the
    starter file can't create false-positive matches when grading code."""
    if not code:
        return ""
    # Drop triple-quoted strings (docstrings / block comments).
    code = re.sub(r'"""[\s\S]*?"""', "", code)
    code = re.sub(r"'''[\s\S]*?'''", "", code)
    # Drop everything after a # on each line.
    code = re.sub(r"#.*", "", code)
    return code


def list_screenshots(directory):
    if not os.path.isdir(directory):
        return []
    return [f for f in os.listdir(directory)
            if f.lower().endswith((".png", ".jpg", ".jpeg"))]


def render(report, checks):
    """Score a list of (task, passed, message, points) and append the table."""
    total = 0
    max_score = 0
    for task, passed, message, points in checks:
        max_score += points
        earned = points if passed else 0
        total += earned
        report.append(f"| {task} | {message} | {earned}/{points} | {'✅' if passed else '❌'} |")
    _append_summary(report, total, max_score)
    return "\n".join(report)


# ═════════════════════════════════════════════════════════════
#  WEEK 1 — Build the Staging Layer
# ═════════════════════════════════════════════════════════════

def grade_week_1():
    report = ["# 📊 Week 1 — Grade Report\n", "## Build the Staging Layer\n",
              "| Task | Check | Points | Status |", "| :--- | :--- | :---: | :---: |"]
    checks = []
    results = load_dbt_results()

    # ── Task 1.1: Load Seeds (10 pts) ───────────────────────
    seed_names = ["raw_customers", "raw_products", "raw_orders",
                  "raw_order_items", "raw_store_locations"]
    seeds_ok = results is not None and all(
        any(s in r.get("unique_id", "") and r.get("status") in ("pass", "success")
            for r in results.get("results", []))
        for s in seed_names
    )
    checks.append(("1.1", seeds_ok,
                   "✅ All 5 seeds loaded" if seeds_ok
                   else "⏳ Seeds not all passing — run `dbt seed` then re-grade", 5))
    checks.append(("1.1", *check_file_contains(
        DBT_PROJECT_YML, r"\+schema:\s*RAW", "Seeds configured to land in RAW schema"), 5))

    # ── Task 1.2: Build 5 Staging Models (60 pts) ───────────
    for model in STAGE_MODELS:
        checks.append(("1.2", *check_file_exists(
            os.path.join(STAGE_DIR, f"{model}.sql"), f"{model}.sql exists"), 6))
    # Casting
    prod = file_exists(os.path.join(STAGE_DIR, "stg_products.sql")) or ""
    orders = file_exists(os.path.join(STAGE_DIR, "stg_orders.sql")) or ""
    cust = file_exists(os.path.join(STAGE_DIR, "stg_customers.sql")) or ""
    checks.append(("1.2", *check_text_contains(
        prod, r"numeric|::", "stg_products casts types (numeric / ::)"), 5))
    checks.append(("1.2", *check_text_contains(
        orders, r"::(date|integer|numeric)", "stg_orders casts types"), 5))
    # Trim / lower / coalesce
    checks.append(("1.2", *check_text_contains(
        cust, r"lower\s*\(|trim\s*\(", "stg_customers uses lower()/trim()"), 5))
    checks.append(("1.2", *check_text_contains(
        orders, r"coalesce\s*\(", "stg_orders uses coalesce() for null defaults"), 5))
    # CTE structure
    checks.append(("1.2", *check_text_contains(
        cust, r"with[\s\S]+as\s*\(", "Clean CTE structure (with … as (…))"), 10))

    # ── Task 1.3: Build the Three Dimensions (30 pts) ───────
    dim_cust = file_exists(os.path.join(DEV_DIR, "dim_customers.sql")) or ""
    dim_prod = file_exists(os.path.join(DEV_DIR, "dim_products.sql")) or ""
    checks.append(("1.3", *check_text_contains(
        dim_cust, r"\|\|", "dim_customers builds full_name via concatenation (||)"), 5))
    checks.append(("1.3", *check_dbt_result(results, "dim_customers", "dim_customers builds successfully"), 5))
    checks.append(("1.3", *check_text_contains(
        dim_prod, r"unit_margin", "dim_products has unit_margin"), 5))
    checks.append(("1.3", *check_dbt_result(results, "dim_products", "dim_products builds successfully"), 5))
    checks.append(("1.3", *check_file_exists(
        os.path.join(DEV_DIR, "dim_stores.sql"), "dim_stores.sql exists"), 5))
    checks.append(("1.3", *check_dbt_result(results, "dim_stores", "dim_stores builds successfully"), 5))

    return render(report, checks)


# ═════════════════════════════════════════════════════════════
#  WEEK 2 — Dimensions & the Incremental Fact
# ═════════════════════════════════════════════════════════════

def grade_week_2():
    report = ["# 📊 Week 2 — Grade Report\n", "## The Incremental Fact & Snapshots\n",
              "| Task | Check | Points | Status |", "| :--- | :--- | :---: | :---: |"]
    checks = []
    results = load_dbt_results()

    fct = os.path.join(DEV_DIR, "fct_order_items.sql")
    fct_c = file_exists(fct) or ""

    # ── Task 2.1: Line-Level Fact (40 pts) ──────────────────
    checks.append(("2.1", *check_file_exists(fct, "fct_order_items.sql exists"), 5))
    checks.append(("2.1", *check_text_contains(fct_c, r"\bjoin\b", "Joins staged tables"), 10))
    for measure in ["gross_amount", "discount_amount", "net_amount", "total_cost", "margin"]:
        checks.append(("2.1", *check_text_contains(fct_c, rf"\b{measure}\b", f"Computes {measure}"), 3))
    checks.append(("2.1", *check_dbt_result(results, "fct_order_items", "fct_order_items builds successfully"), 10))

    # ── Task 2.2: Make it Incremental (30 pts) ──────────────
    checks.append(("2.2", *check_text_contains(
        fct_c, r"materialized\s*=\s*['\"]incremental['\"]", "materialized='incremental'"), 10))
    checks.append(("2.2", *check_text_contains(
        fct_c, r"unique_key\s*=\s*['\"]order_item_id['\"]", "unique_key='order_item_id'"), 10))
    checks.append(("2.2", *check_text_contains(
        fct_c, r"is_incremental\s*\(\s*\)", "{% if is_incremental() %} block present"), 10))

    # ── Task 2.3: Snapshot Product Prices (30 pts) ──────────
    snap = os.path.join(DBT_PROJECT_DIR, "snapshots", "snap_products.sql")
    snap_c = file_exists(snap) or ""
    checks.append(("2.3", *check_file_exists(snap, "snapshots/snap_products.sql exists"), 10))
    checks.append(("2.3", *check_text_contains(
        snap_c, r"\{%\s*snapshot", "{% snapshot %} block present"), 5))
    checks.append(("2.3", *check_text_contains(
        snap_c, r"unique_key\s*=\s*['\"]product_id['\"]", "unique_key='product_id'"), 5))
    checks.append(("2.3", *check_text_contains(
        snap_c, r"strategy\s*=\s*['\"]check['\"]", "strategy='check'"), 5))
    checks.append(("2.3", *check_text_contains(
        snap_c, r"check_cols[\s\S]{0,60}list_price", "check_cols includes list_price"), 5))

    return render(report, checks)


# ═════════════════════════════════════════════════════════════
#  WEEK 3 — Test the Warehouse
# ═════════════════════════════════════════════════════════════

def grade_week_3():
    report = ["# 📊 Week 3 — Grade Report\n", "## Test the Warehouse\n",
              "| Task | Check | Points | Status |", "| :--- | :--- | :---: | :---: |"]
    checks = []
    results = load_dbt_results()

    yaml_blob = concat_dir_content(MODELS_DIR)
    project_c = file_exists(DBT_PROJECT_YML) or ""

    # ── Task 3.1: Generic Tests in YAML (40 pts) ────────────
    checks.append(("3.1", bool(re.search(r"tests:", yaml_blob)),
                   "✅ A schema .yml with tests exists under models/"
                   if re.search(r"tests:", yaml_blob)
                   else "❌ No schema .yml with tests found under models/", 5))
    checks.append(("3.1", *check_text_contains(yaml_blob, r"-\s*unique", "Contains 'unique' tests"), 10))
    checks.append(("3.1", *check_text_contains(yaml_blob, r"-\s*not_null", "Contains 'not_null' tests"), 10))
    checks.append(("3.1", *check_text_contains(yaml_blob, r"relationships", "Contains 'relationships' test"), 10))
    both_declared = (re.search(r"name:\s*dim_customers", yaml_blob) and
                     re.search(r"name:\s*fct_order_items", yaml_blob))
    checks.append(("3.1", bool(both_declared),
                   "✅ dim_customers and fct_order_items declared in schema"
                   if both_declared else "❌ dim_customers / fct_order_items not both declared", 5))

    # ── Task 3.2: One Custom Test (20 pts) ──────────────────
    tests_blob = ""
    custom_file_found = False
    if os.path.isdir(TESTS_DIR):
        for fname in os.listdir(TESTS_DIR):
            if fname.endswith(".sql"):
                c = file_exists(os.path.join(TESTS_DIR, fname)) or ""
                tests_blob += "\n" + c
                if re.search(r"net_amount", c, re.IGNORECASE):
                    custom_file_found = True
    checks.append(("3.2", custom_file_found,
                   "✅ Custom test on net_amount found in tests/"
                   if custom_file_found else "❌ No custom test referencing net_amount in tests/", 6))
    checks.append(("3.2", *check_text_contains(tests_blob, r"ref\s*\(", "Custom test uses ref()"), 4))
    checks.append(("3.2", *check_text_contains(
        tests_blob, r"net_amount\s*<\s*0", "Checks net_amount < 0"), 6))
    checks.append(("3.2", *check_text_contains(
        tests_blob, r"severity\s*[=:]\s*['\"]?warn",
        "Custom test set to severity='warn' (keeps Week 6 pipeline green)"), 4))

    # ── Task 3.3: Fix the Duplicate Orders Bug (25 pts) ─────
    stg_orders = file_exists(os.path.join(STAGE_DIR, "stg_orders.sql")) or ""
    checks.append(("3.3", *check_text_contains(
        stg_orders, r"row_number\s*\(\s*\)|distinct", "stg_orders deduplicates (row_number/distinct)"), 15))
    checks.append(("3.3", *check_model_rows(results, "fct_order_items", 313, "fct_order_items = 313 rows"), 10))

    # ── Task 3.4: Store Test Failures (15 pts) ──────────────
    # store_failures may be set per-test in schema.yml or project-wide in
    # dbt_project.yml — accept either location.
    store_blob = yaml_blob + "\n" + project_c
    checks.append(("3.4", *check_text_contains(
        store_blob, r"store_failures", "store_failures configured (schema.yml or dbt_project.yml)"), 10))
    checks.append(("3.4", *check_text_contains(
        store_blob, r"store_failures\s*:\s*true", "store_failures set to true"), 5))

    return render(report, checks)


# ═════════════════════════════════════════════════════════════
#  WEEK 4 — A Reusable Macro
# ═════════════════════════════════════════════════════════════

def grade_week_4():
    report = ["# 📊 Week 4 — Grade Report\n", "## A Reusable Macro\n",
              "| Task | Check | Points | Status |", "| :--- | :--- | :---: | :---: |"]
    checks = []
    results = load_dbt_results()

    macro_path = os.path.join(MACROS_DIR, "net_amount.sql")
    macro_c = file_exists(macro_path) or ""
    fct = os.path.join(DEV_DIR, "fct_order_items.sql")
    fct_c = file_exists(fct) or ""
    orders = os.path.join(DEV_DIR, "fct_orders.sql")
    orders_c = file_exists(orders) or ""

    # ── Task 4.1: Write the Macro (45 pts) ──────────────────
    checks.append(("4.1", *check_file_exists(macro_path, "macros/net_amount.sql exists"), 10))
    checks.append(("4.1", *check_text_contains(
        macro_c, r"\{%\s*macro\s+net_amount", "Defines net_amount macro"), 15))
    has_args = all(re.search(a, macro_c, re.IGNORECASE)
                   for a in [r"quantity", r"unit_price", r"discount"])
    checks.append(("4.1", has_args,
                   "✅ Macro takes quantity, unit_price, discount args" if has_args
                   else "❌ Macro missing one of quantity/unit_price/discount args", 10))
    checks.append(("4.1", *check_dbt_result(results, "fct_order_items", "Project compiles (fct_order_items builds)"), 10))

    # ── Task 4.2: Use It in the Fact (35 pts) ───────────────
    checks.append(("4.2", *check_text_contains(
        fct_c, r"\{\{\s*net_amount\s*\(", "fct_order_items calls net_amount() macro"), 20))
    checks.append(("4.2", *check_dbt_result(results, "fct_order_items", "fct_order_items still builds"), 15))

    # ── Task 4.3: Build fct_orders (20 pts) ─────────────────
    rollup = re.search(r"group\s+by", orders_c, re.IGNORECASE) and re.search(r"sum\s*\(", orders_c, re.IGNORECASE)
    checks.append(("4.3", bool(rollup),
                   "✅ fct_orders rolls up (group by + sum)" if rollup
                   else "❌ fct_orders needs group by + sum() rollup", 10))
    checks.append(("4.3", *check_text_contains(orders_c, r"order_total", "Has order_total (net + shipping)"), 5))
    checks.append(("4.3", *check_model_rows(results, "fct_orders", 155, "fct_orders = 155 rows"), 5))

    return render(report, checks)


# ═════════════════════════════════════════════════════════════
#  WEEK 5 — Speed It Up
# ═════════════════════════════════════════════════════════════

def grade_week_5():
    report = ["# 📊 Week 5 — Grade Report\n", "## Hooks\n",
              "| Task | Check | Points | Status |", "| :--- | :--- | :---: | :---: |"]
    checks = []
    results = load_dbt_results()

    fct = os.path.join(DEV_DIR, "fct_order_items.sql")
    fct_c = file_exists(fct) or ""
    project_c = file_exists(DBT_PROJECT_YML) or ""

    # ── Task 5.1: Post-Hook Index (50 pts) ──────────────────
    checks.append(("5.1", *check_text_contains(fct_c, r"post_hook", "fct_order_items has a post_hook"), 20))
    checks.append(("5.1", *check_text_contains(
        fct_c, r"create\s+index\s+if\s+not\s+exists", "Index uses CREATE INDEX IF NOT EXISTS"), 15))
    checks.append(("5.1", *check_text_contains(
        fct_c, r"create\s+index[\s\S]{0,120}order_id", "Index is on order_id"), 15))

    # ── Task 5.2: Project-Level GRANT Hook (35 pts) ─────────
    checks.append(("5.2", *check_text_contains(
        project_c, r"\+post[_-]hook", "Project-level +post-hook in dbt_project.yml"), 15))
    checks.append(("5.2", *check_text_contains(
        project_c, r"grant\s+select\s+on\s+\{\{\s*this\s*\}\}", "GRANT SELECT ON {{ this }} present"), 15))
    checks.append(("5.2", *check_dbt_result(results, "fct_order_items", "Models still build with the hook"), 5))

    # ── Task 5.3: Measure the Index (15 pts) ────────────────
    notes_path = os.path.join(REPO_ROOT, "week_5", "notes.md")
    notes_c = file_exists(notes_path) or ""
    checks.append(("5.3", *check_file_exists(notes_path, "week_5/notes.md exists"), 5))
    has_timing = re.search(r"\bms\b|milliseconds|execution time|\d+\.\d+\s*ms|explain", notes_c, re.IGNORECASE)
    checks.append(("5.3", bool(has_timing),
                   "✅ notes.md records timing evidence" if has_timing
                   else "❌ notes.md needs before/after timing numbers", 10))

    return render(report, checks)


# ═════════════════════════════════════════════════════════════
#  WEEK 6 — Automate with Airflow
# ═════════════════════════════════════════════════════════════

def grade_week_6():
    report = ["# 📊 Week 6 — Grade Report\n", "## Automate with Airflow\n",
              "| Task | Check | Points | Status |", "| :--- | :--- | :---: | :---: |"]
    checks = []

    dag_path = os.path.join(AIRFLOW_DAGS_DIR, "dbt_pipeline.py")
    dag_raw = file_exists(dag_path) or ""
    dag_c = strip_py_comments(dag_raw)   # ignore TODO hints in comments

    # ── Task 6.1: Read & Run the DAG (30 pts) ───────────────
    shots = list_screenshots(os.path.join(REPO_ROOT, "week_6"))
    checks.append(("6.1", len(shots) >= 1,
                   f"✅ {len(shots)} screenshot(s) of the DAG run in week_6/"
                   if shots else "❌ No screenshot in week_6/ (graph view, all tasks green)", 30))

    # ── Task 6.2: Add a Build Task (50 pts) ─────────────────
    checks.append(("6.2", *check_file_exists(dag_path, "airflow/dags/dbt_pipeline.py exists"), 5))
    checks.append(("6.2", *check_text_contains(dag_c, r"BashOperator", "DAG uses BashOperator"), 10))
    checks.append(("6.2", *check_text_contains(dag_c, r"dbt_build", "dbt_build task defined"), 15))
    checks.append(("6.2", *check_text_contains(
        dag_c, r"dbt\s+build|['\"]build['\"]", "Task runs `dbt build`"), 10))
    wired = re.search(r"dbt_build\s*(>>|<<)|(>>|<<)\s*dbt_build", dag_c)
    checks.append(("6.2", bool(wired),
                   "✅ dbt_build is wired into the dependency chain" if wired
                   else "❌ dbt_build is not wired in with >> ", 10))

    # ── Task 6.3: Retry Config (20 pts) ─────────────────────
    checks.append(("6.3", *check_text_contains(dag_c, r"retries['\"]?\s*[:=]\s*2", "retries=2"), 8))
    checks.append(("6.3", *check_text_contains(dag_c, r"retry_delay['\"]?\s*[:=]", "retry_delay configured"), 4))
    checks.append(("6.3", *check_text_contains(dag_c, r"catchup\s*=\s*False", "catchup=False"), 8))

    return render(report, checks)


# ═════════════════════════════════════════════════════════════
#  SHARED
# ═════════════════════════════════════════════════════════════

def _append_summary(report, total, max_score):
    pct = (total / max_score * 100) if max_score > 0 else 0
    report.append(f"\n## **Total Score: {total} / {max_score}  ({pct:.0f}%)**")
    if pct >= 90:
        report.append("\n🟢 **Excellent Work!** Clean and correct.")
    elif pct >= 75:
        report.append("\n🔵 **Good progress.** Review the failing checks to reach 90%+.")
    elif pct >= 60:
        report.append("\n🟡 **Satisfactory.** Core tasks present but gaps remain.")
    else:
        report.append("\n🔴 **Needs Work.** Significant gaps — review the assignment instructions.")


# ═════════════════════════════════════════════════════════════
#  MAIN
# ═════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(description="DataOps Mentorship — Assignment Grader")
    parser.add_argument("--week", type=int, required=True, choices=[1, 2, 3, 4, 5, 6],
                        help="Which week to grade (1–6)")
    args = parser.parse_args()

    graders = {
        1: grade_week_1, 2: grade_week_2, 3: grade_week_3,
        4: grade_week_4, 5: grade_week_5, 6: grade_week_6,
    }
    print(graders[args.week]())


if __name__ == "__main__":
    main()
