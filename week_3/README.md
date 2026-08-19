# Week 3: Test the Warehouse

This week you **validate your data** with dbt's testing framework — and you'll discover that the raw data isn't as clean as it looks. Budget: **~2 hours**.

---

## 📖 Lesson Overview

*   **Generic Tests:** `not_null`, `unique`, and `relationships` declared in YAML.
*   **Custom Singular Tests:** a SQL query that returns "bad" rows — if it returns anything, the test fails.
*   **Finding & Fixing a Bug:** one of your tests will fail on real bad data. Your job is to diagnose it and fix the model.
*   **Test Severity:** decide whether a failing test should **stop the pipeline** or just **warn**.

> 🕵️ The raw data contains some intentional quality issues. Don't go hunting for them — let your tests surface them.

---

## 🎚️ Test Severity: `error` vs `warn`

Every dbt test has a **severity**. This matters a lot once Week 6 runs your pipeline in Airflow.

| Severity | On failure | dbt exit code | Airflow task |
|---|---|---|---|
| `error` (default) | fails, and **skips downstream models** | non-zero | 🔴 turns the task red |
| `warn` | prints a `WARN` but keeps going | zero | 🟢 stays green |

So the rule of thumb:

*   Use **`error`** for things that must *never* happen — a duplicate primary key, a null ID. You want the pipeline to **stop**.
*   Use **`warn`** for **known-dirty source data you can't fix** — like orphaned foreign keys or the odd bad discount. You still want to *see* it, but it shouldn't break every run.

Set it with a `config` block on any test:
```yaml
- relationships:
    to: ref('dim_products')
    field: product_id
    config:
      severity: warn
```

> ⚠️ **Why this matters:** in Week 6 your Airflow DAG runs `dbt build`. If a test that catches *unfixable* dirty data is left at `error` severity, that task turns **red** and the whole DAG fails. Put those on `warn` so your pipeline stays green while still surfacing the issue.

---

## 📝 Assignment Tasks

### Task 3.1 — Generic Tests in YAML (40 pts)
Create `models/schema.yml` and add generic tests:

| Model | Required Tests |
|---|---|
| `dim_customers` | `unique` + `not_null` on `customer_id` |
| `dim_products` | `unique` + `not_null` on `product_id` |
| `dim_stores` | `unique` + `not_null` on `store_id` |
| `stg_orders` | `unique` + `not_null` on `order_id` |
| `fct_order_items` | `not_null` on `order_item_id`; `relationships` to `dim_products` and `dim_customers` |

**💡 Example:**
```yaml
version: 2

models:
  - name: dim_customers
    columns:
      - name: customer_id
        tests:
          - unique
          - not_null
```

Run the tests:
```bash
dbt test --profiles-dir .
```
**Deliverable:** `schema.yml` + a note of which tests fail and why. **Some tests SHOULD fail** — that's the point. The two `relationships` tests on `fct_order_items` will flag orphaned rows from the dirty source data — set those to **`severity: warn`** (see above) so they don't break your Week 6 pipeline.

### Task 3.2 — One Custom Test (20 pts)
Write a singular test in `tests/` that returns any rows where `net_amount < 0` in `fct_order_items`.

**💡 Hint:** a singular test is just a SELECT — any rows it returns are failures.
```sql
-- tests/test_net_amount_non_negative.sql
{{ config(severity='warn') }}

select *
from {{ ref('fct_order_items') }}
where net_amount < 0
```

This test *will* find bad rows (a couple of records have a 150% discount, making `net_amount` negative). Since that's dirty source data you're not fixing here, give the test **`severity='warn'`** — it surfaces the problem in the logs but keeps `dbt build` green when Airflow runs it in Week 6.

**Deliverable:** the test file under `tests/`, set to `warn` severity.

### Task 3.3 — Find & Fix the Duplicate Orders Bug (25 pts)
Your `unique` test on `stg_orders.order_id` **fails** — one `order_id` appears twice in the raw feed. Because of it, the line-item fact fans out to more rows than there are order items.

Fix it by **deduplicating in `stg_orders`** so each `order_id` appears once. A common pattern:

```sql
deduped as (
    select *
    from (
        select *,
               row_number() over (partition by order_id order by order_date desc) as rn
        from cleaned
    ) ranked
    where rn = 1
)
```

**Deliverable:** a fixed `stg_orders`. After the fix:
*   the `unique` test on `order_id` passes,
*   `fct_order_items` = **313 rows** and `fct_orders` = **155 rows**.

### Task 3.4 — Store the Failures (15 pts)
A failing test tells you *how many* rows are bad, but not *which* ones. Turn on **`store_failures`** so dbt saves each failing test's rows into a table you can query.

Configure it per-test in `schema.yml`:
```yaml
- not_null:
    config:
      store_failures: true
```
…or project-wide in `dbt_project.yml`:
```yaml
data_tests:
  dbt_learning:
    +store_failures: true
```

Then run and inspect:
```bash
dbt test --store-failures --profiles-dir .
```
```sql
-- failures land in a *_dbt_test__audit style schema
select * from "DEV_dbt_test__audit".not_null_fct_order_items_order_item_id;
```

**Deliverable:** `store_failures` enabled, and you can point to a table holding the actual bad rows.

---

## 🤖 Auto-Grade Your Work

```bash
python scripts/grade_assignment.py --week 3
```

Fix any ❌ items and re-run. 🚀
