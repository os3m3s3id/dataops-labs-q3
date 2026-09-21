# Week 5 — Task 5.3: Measuring the Index

## Query

\`\`\`sql
explain analyze
select *
from "DEV".fct_order_items f
join "DEV".fct_orders o on f.order_id = o.order_id;
\`\`\`

## Results

**Before (index dropped):** Execution Time: 1.208 ms — Seq Scan on fct_order_items
**After (index restored):** Execution Time: 0.292 ms — Seq Scan on fct_order_items

## Interpretation

Even with the index in place, Postgres's planner chose a Seq Scan in both cases —
it never used the index at all. On a table this small (313 rows), scanning the
whole table is cheaper than the overhead of an index lookup, so the planner
correctly ignores the index regardless of whether it exists. The observed timing
difference is most likely cache-warmth variance between runs rather than any
effect of the index itself, since the query plan shape is identical before and
after. This is the expected outcome for a table this size — the index would only
start mattering once row counts grow large enough for a Seq Scan to become the
more expensive option.