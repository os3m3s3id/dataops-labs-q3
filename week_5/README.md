# Week 5: Hooks

This week you make dbt run SQL **around** your model builds — not just the `select` inside them. You'll attach an index to `fct_order_items` with a **post-hook**, add a project-wide `GRANT` so every model is readable by dashboards, and then prove the index actually did something. Budget: **~2 hours**.

---

## 📖 Lesson Overview

*   **Post-Hooks:** a snippet of SQL dbt runs **immediately after** a model finishes building — perfect for things a `select` statement can't express, like creating an index.
*   **Project-Level Hooks:** the same idea configured once in `dbt_project.yml` so it applies to *every* model, instead of being repeated in each file.
*   **Measuring It:** `EXPLAIN ANALYZE` before and after, so you know the index earned its place.

---

## 🪝 Where Hooks Live (and Why the Spelling Changes)

A hook can be declared in two places, and **the two spellings are different** — this trips everyone up once:

| Where | Spelling | Applies to |
|---|---|---|
| Inside a model's `{{ config(...) }}` | `post_hook` (**underscore**) | just that one model |
| In `dbt_project.yml` | `+post-hook` (**hyphen**, with the `+`) | every model under that path |

> 💡 They **stack, they don't override.** If a model has its own `post_hook` *and* the project defines a `+post-hook`, dbt runs **both**. Adding the project-level grant in Task 5.2 will not clobber your index hook from Task 5.1.

There's also `pre_hook` / `+pre-hook` (runs *before* the model) — same rules, you just won't need it this week.

---

## 📝 Assignment Tasks

### Task 5.1 — Post-Hook: Create an Index (50 pts)

`fct_orders` joins `fct_order_items` on `order_id` every time it builds. Give Postgres an index to make that join cheap — and have **dbt** create it, so it's part of the build rather than something you remember to run by hand.

Add a `post_hook` to the `config()` block in `models/dev/fct_order_items.sql`:

```sql
{{ config(
    materialized='incremental',
    unique_key='order_item_id',
    post_hook="create index if not exists idx_fct_order_items_order_id on {{ this }} (order_id)"
) }}
```

Two details worth understanding:

*   **`{{ this }}`** resolves to the fully-qualified, correctly-quoted name of the model dbt just built (`"ecommerce"."DEV"."fct_order_items"`). Never hardcode the schema — `{{ this }}` keeps the hook correct in any target. The nested `{{ }}` inside the `config()` block is fine: hooks are stored as strings and rendered later, when they run.
*   **`if not exists`** makes the hook **idempotent**. The hook fires on *every* `dbt run`, and a plain `create index` would error the second time. This matters especially because `fct_order_items` is **incremental** — the table survives between runs, so the index (and the name collision) survives with it.

Verify it landed by querying Postgres's index catalog directly:

```sql
select indexname, indexdef
from pg_indexes
where tablename = 'fct_order_items';
```

**Deliverable:** `fct_order_items` builds (`dbt run --select fct_order_items --profiles-dir .`), a second run succeeds too, and `idx_fct_order_items_order_id` shows up in `pg_indexes`.

### Task 5.2 — Project-Level GRANT Hook (35 pts)

Right now only the `dataops` user can read your tables. A BI tool connecting as anyone else sees nothing. Rather than adding a `GRANT` to all eleven models by hand, declare it **once** at the project level.

In `dbt_project.yml`, under the `models:` block, add a `+post-hook` so it applies to every model in the project:

```yaml
models:
  dbt_learning:
    +post-hook: "GRANT SELECT ON {{ this }} TO PUBLIC;"

    stage:
      +schema: STAGE
      +materialized: view
    ...
```

> ⚠️ **Quote the string.** A bare YAML value can't start with `{`, and quoting keeps the `{{ this }}` template safe from the YAML parser. Put the `+post-hook` at the `dbt_learning:` level (not inside `stage:` or `dev:`) so it covers both layers.

Then rebuild everything and confirm nothing broke:

```bash
dbt run --profiles-dir .
```

**Deliverable:** the `+post-hook` in `dbt_project.yml`, and a full `dbt run` still green — every model, in both `STAGE` and `DEV`, now grants read access as it builds.

### Task 5.3 — Measure the Index (15 pts)

An index you never measured is a guess. Time the join both ways and write down what you actually saw.

**1. Drop the index** so you're measuring the "before" state:

```sql
drop index if exists "DEV".idx_fct_order_items_order_id;
```

**2. Time the join** with `EXPLAIN ANALYZE` — look at the `Execution Time` line at the bottom, and note which scan type Postgres picked (`Seq Scan` vs. `Index Scan`):

```sql
explain analyze
select *
from "DEV".fct_order_items f
join "DEV".fct_orders o on f.order_id = o.order_id;
```

**3. Put the index back** by re-running the model (`dbt run --select fct_order_items --profiles-dir .`), then run the exact same `EXPLAIN ANALYZE` again.

**4. Record both numbers** in a new file, `week_5/notes.md` — the query you ran, the before/after execution times, and a sentence on what changed in the plan.

> 🔍 **Report what you actually measure.** This is a 313-row table, so the numbers will be small (single-digit milliseconds) and the planner may well *still* choose a sequential scan — on tiny tables a seq scan is genuinely cheaper than an index lookup. "No meaningful improvement, because the table is too small for the planner to bother" is a **correct and full-credit answer**, as long as it's backed by your real numbers. Run each query two or three times and use the later timings; the first run pays for a cold cache.

**Deliverable:** `week_5/notes.md` with your before/after `Execution Time` figures and a short interpretation.

---

## 🤖 Auto-Grade Your Work

```bash
python scripts/grade_assignment.py --week 5
```

Fix any ❌ items and re-run. 🚀
