# Week 4: A Reusable Macro

This week you stop repeating yourself. The `net_amount` formula shows up more than once in your fact model — you'll pull it into a **Jinja macro** and reuse it, then use that same reuse instinct to build a second fact table. Budget: **~2 hours**.

---

## 📖 Lesson Overview

*   **Jinja Basics:** a macro is just a parameterized snippet of SQL, called with `{{ macro_name(args) }}` — the same `{{ }}` syntax you've already used for `ref()`.
*   **Your First Macro:** wrap the `net_amount` math in `macros/net_amount.sql` and call it from `fct_order_items` instead of writing the formula out by hand.
*   **Reuse It:** apply the same "don't repeat yourself" instinct at a bigger scale — roll `fct_order_items` up into an order-grain `fct_orders`.

---

## 📝 Assignment Tasks

### Task 4.1 — Write the Macro (45 pts)
Create `macros/net_amount.sql`:

```sql
-- Usage: {{ net_amount('quantity', 'unit_price', 'discount_pct') }}
-- Returns: quantity * unit_price * (1 - discount_pct / 100.0)
{% macro net_amount(quantity, unit_price, discount_pct) %}
    ({{ quantity }} * {{ unit_price }} * (1 - {{ discount_pct }} / 100.0))
{% endmacro %}
```

**💡 Note:** the arguments are passed in as strings (column names), not values — the macro just stitches them into SQL text. That's why the call site looks like `net_amount('oi.quantity', 'oi.unit_price', 'oi.discount_pct')`, quotes and all.

**Deliverable:** `macros/net_amount.sql`, taking `quantity`, `unit_price`, and `discount_pct` and compiling cleanly (`dbt compile --profiles-dir .`).

### Task 4.2 — Use It in the Fact (35 pts)
In `fct_order_items`, replace the hardcoded `net_amount` formula (and anywhere else you repeated it, e.g. inside the `margin` calc) with a call to your macro:

```sql
{{ net_amount('oi.quantity', 'oi.unit_price', 'oi.discount_pct') }}::numeric(12,2) as net_amount
```

**Deliverable:** `fct_order_items` calls `{{ net_amount(...) }}` instead of repeating the formula, and still builds — output is **byte-for-byte unchanged** from before (`dbt run --profiles-dir .`).

### Task 4.3 — Reuse It Once More: Build `fct_orders` (20 pts)
Create `models/dev/fct_orders.sql` — an **order-grain** fact (one row per order) that rolls up `fct_order_items`:

*   Group the line items by `order_id` and `sum()` up `num_line_items`, `total_quantity`, and `net_revenue`.
*   Bring in the order header's `shipping_fee`.
*   Add `order_total = net_revenue + shipping_fee`.

**💡 Hint:** join back to `stg_orders` for the header columns (`customer_id`, `store_id`, `order_date`, `shipping_fee`), and `left join` your rollup so orders with no items still show up.

**Deliverable:** `fct_orders.sql` builds and returns **155 rows** — one per order.

---

## 🤖 Auto-Grade Your Work

```bash
python scripts/grade_assignment.py --week 4
```

Fix any ❌ items and re-run. 🚀
