# PRD: Build Silver (Integration) and Gold (Datamart) layers for medallion demo template

## Problem Statement

The Databricks demo template currently only fully implements the Bronze (staging) layer. The Integration (Silver) and Datamart (Gold) layers are placeholders, and the AI/BI consumption surface is missing entirely. To deliver a production-grade demo arc that showcases Databricks platform features end-to-end — DLT Expectations, Change Data Feed, `apply_changes`, Materialised Views, Liquid Clustering, AI/BI Dashboards, AI/BI Genie — the full medallion architecture needs to be designed and built.

Bronze cleanup is **already complete**: the ingest notebook now has `mode` (`full`/`incremental`/`both`) and `reset` widgets, the Workflow runs `ingest_full ‖ ingest_incremental` in parallel, DLT has been removed from staging, and every Bronze table is created with `delta.enableChangeDataFeed=true`. Silver and Gold can build on a stable Bronze foundation.

## Solution

Add a **Silver DLT pipeline** that consumes Bronze via Change Data Feed + DLT `apply_changes`, applies type fixes and snake_case naming, enforces data quality via warn/drop/fail Expectations with paired `_quarantine` tables, and produces an integrated `sales_line` view.

Add a **Gold DLT pipeline** that produces three KPI aggregates plus a wide AI/BI-friendly table with Liquid Clustering, all read-optimised for Dashboards and Genie.

Add a **serialized AI/BI Dashboard** that auto-deploys via DAB, plus a documented runbook for the Genie post-deploy setup.

The full design is captured in `docs/adr/0001-*` and `docs/adr/0002-*`.

## User Stories

1. As a demo presenter, I want to deploy the entire stack with one `databricks bundle deploy` command, so that I can show clients a complete medallion architecture without manual setup steps.
2. As a demo presenter, I want to switch a table's `load_type` from full to incremental with one UPDATE and have Silver continue working seamlessly, so that I can show the metadata-driven pipeline arc end-to-end.
3. As a demo presenter, I want to demonstrate Change Data Feed propagating writes from Bronze to Silver, so that clients see Databricks' CDC capabilities first-hand.
4. As a demo presenter, I want to show DLT Expectations dropping rows into a tangible `_quarantine` table during a live run, so that data quality enforcement is visible and queryable rather than hidden in an event log.
5. As a demo presenter, I want a pre-built Lakeview dashboard with KPI cards and trend charts that auto-deploys with the bundle, so that I can show executive-level insights immediately after deploy.
6. As a demo presenter, I want to point AI/BI Genie at the wide Gold table and ask plain-language questions, so that clients see Databricks' natural-language analytics capability.
7. As a demo presenter, I want a documented runbook for the Genie space setup in `docs/demo_script.md`, so that I don't have to invent it during the demo.
8. As a demo presenter, I want to compare Bronze and Silver side-by-side to show raw int-millis vs human-readable `'HH:mm:ss'`, so that the value of the Integration layer is concrete.
9. As a data analyst, I want to query the cleansed `INTEGRATION` tables with snake_case English column names, so that I can write SQL without bron-jargon.
10. As a data analyst, I want decimal types on revenue/tax/discount columns (not strings), so that aggregations compute correctly without re-casts.
11. As a data analyst, I want time-of-day shift columns in `'HH:mm:ss'` format, so that I can read them at a glance without parsing milliseconds-since-midnight.
12. As a data analyst, I want to query `INTEGRATION.sales_line` as the integrated business view, so that I don't have to join `order_header` and `order_detail` myself.
13. As a data analyst, I want to query `_quarantine` tables to find data quality issues, so that I can triage upstream problems.
14. As a data analyst, I want a `failed_rules` array column on quarantine rows showing which rules they violated, so that I can filter to specific quality failures with `array_contains(...)`.
15. As a data analyst, I want pre-aggregated daily/monthly metrics in `DATAMART`, so that ad-hoc dashboards run fast against small, focused tables.
16. As a business analyst, I want to view a Lakeview dashboard with revenue trends, top trucks, and top locations, so that I can monitor business health without writing SQL.
17. As a business analyst, I want to ask Genie "Which truck made the most revenue last week?" and get an answer in seconds, so that I can explore data without writing SQL.
18. As a business analyst, I want to see orphaned-truck revenue as an explicit "Unknown" bucket in the aggregates, so that I'm aware of attribution gaps rather than silently missing the data.
19. As a data engineer, I want the Silver DLT pipeline to handle Bronze full-load overwrites without duplication, so that mode switches don't corrupt downstream data.
20. As a data engineer, I want type-fix logic in pure helper functions, so that I can reason about transformations without reading the entire pipeline notebook.
21. As a data engineer, I want the rule engine to take a `{rule_name: predicate}` dict, so that adding/removing data quality rules is a one-line change.
22. As a data engineer, I want the Workflow to chain `setup → ingest_* → dlt_integration → dlt_datamart`, so that one trigger runs the entire medallion stack.
23. As a data engineer, I want the Gold pipeline to be separately triggerable from Silver, so that I can refresh Gold independently when only Gold logic changes.
24. As an ML engineer, I want `sales_lines_wide` enriched with derived time columns (`order_hour`, `order_day_of_week`, `order_year_month`, `shift_duration_minutes`), so that I can build features without re-deriving them on every query.
25. As an ML engineer, I want Liquid Clustering on the wide Gold table, so that filter-heavy ad-hoc queries are fast without manual partitioning decisions.
26. As a client watching the demo, I want to see the DLT graph view in the Databricks UI with all 5 Silver nodes and their dependencies, so that I understand the lineage at a glance.
27. As a client watching the demo, I want to see the DLT Expectations dashboard with violation counts, so that I see data quality being measured and acted on in real time.
28. As a compliance officer, I want every Bronze table to have audit columns (`_ingestion_timestamp`, `_source_file`, `_pipeline_run_id`), so that data lineage is traceable.
29. As a compliance officer, I want pipeline runs visible in the Workflow run history, so that I can audit who ran what and when.

## Implementation Decisions

### Schemas

- Silver lives in a single `INTEGRATION` schema (not per-source). The whole point of Silver is that source provenance disappears into the Enterprise view.
- Gold lives in a single `DATAMART` schema. One demo, one consumption story.

### Silver tables (5 total)

- `INTEGRATION.order_header` (Streaming Table) — cleansed
- `INTEGRATION.order_header_quarantine` (Streaming Table) — failing rows + `failed_rules` array
- `INTEGRATION.order_detail` (Streaming Table) — cleansed
- `INTEGRATION.order_detail_quarantine` (Streaming Table) — failing rows + `failed_rules` array
- `INTEGRATION.sales_line` (Materialised View) — `order_header ⨝ order_detail` at line grain

### Silver tech

- Lakeflow Declarative Pipelines (DLT) — single pipeline notebook for all 5 tables
- DAB resource defines the pipeline, points at the notebook, targets the `INTEGRATION` schema
- Reads Bronze via `spark.readStream.option("readChangeFeed", "true").table(...)` + DLT `apply_changes` (per ADR 0002)

### Quarantine pattern (Pattern A — paired tables)

- For each cleansed table, a sibling `_quarantine` table partitions the input
- Routing via SQL filter on a shared predicate built from drop-grade rules
- Quarantine rows carry a `failed_rules ARRAY<STRING>` column for triage

### Three severity levels

- `warn` — rule applied as `@dlt.expect_all`; row stays in cleansed, violations counted in DLT events
- `drop` — row routed to `_quarantine` with the failing rules attached
- `fail` — applied as `@dlt.expect_all_or_fail`; pipeline halts (used for invariants like `order_id IS NOT NULL`)

### Type fixes (Bronze → Silver)

| Bronze column | Bronze type | Silver type |
|---|---|---|
| `SERVED_TS` | StringType | TimestampType |
| `ORDER_TAX_AMOUNT` | StringType | DecimalType(38, 4) |
| `ORDER_DISCOUNT_AMOUNT` | StringType | DecimalType(38, 4) |
| `ORDER_ITEM_DISCOUNT_AMOUNT` | StringType | DecimalType(38, 4) |
| `LOCATION_ID` | DoubleType | DecimalType(38, 0) |
| `DISCOUNT_ID` | StringType | DecimalType(38, 0) (nullable) |
| `SHIFT_START_TIME` | IntegerType (millis) | StringType `'HH:mm:ss'` |
| `SHIFT_END_TIME` | IntegerType (millis) | StringType `'HH:mm:ss'` |

Bronze keeps the int-millis as the canonical source representation; Silver presents the human-readable form. Spark/Delta has no native time-of-day type — string is the most readable choice (no `1970-01-01` placeholder noise from a TimestampType hack).

### Silver column naming

- All columns renamed to snake_case + English in Silver (`ORDER_ID` → `order_id`, `SHIFT_START_TIME` → `shift_start_time`, etc.).
- Audit columns from Bronze (`_ingestion_timestamp`, `_source_system`, `_source_file`, `_last_modified`, `_pipeline_run_id`) carry through unchanged.

### Silver rule severities

`order_header`:

| Rule | Severity |
|---|---|
| `order_id IS NOT NULL` | fail |
| `order_ts IS NOT NULL` | drop |
| `customer_id IS NOT NULL` | drop |
| `order_currency IS NOT NULL` | drop |
| `order_total >= 0` | drop |
| `order_amount >= 0` | drop |
| `truck_id IS NOT NULL` | warn |
| `location_id IS NOT NULL` | warn |
| `shift_start_time <= shift_end_time` | warn |

`order_detail`:

| Rule | Severity |
|---|---|
| `order_detail_id IS NOT NULL` | fail |
| `order_id IS NOT NULL` | drop |
| `menu_item_id IS NOT NULL` | drop |
| `quantity > 0` | drop |
| `unit_price >= 0` | drop |
| `price >= 0` | drop |
| `line_number > 0` | warn |

### Gold tables (4 total)

| Table | Type | Source | Grain |
|---|---|---|---|
| `DATAMART.daily_sales_by_truck` | MV | `INTEGRATION.order_header` | (order_date, truck_id) |
| `DATAMART.daily_sales_by_location` | MV | `INTEGRATION.order_header` | (order_date, location_id) |
| `DATAMART.monthly_revenue_by_currency` | MV | `INTEGRATION.order_header` | (year_month, order_currency) |
| `DATAMART.sales_lines_wide` | MV | `INTEGRATION.sales_line` | per sales line |

### Gold tech

- Separate DLT pipeline (not combined with Silver — layer separation, independent refresh)
- DAB resource for the pipeline; targets the `DATAMART` schema
- Aggregates source from `order_header` (order grain) to avoid `SUM`-over-duplicated-line-rows; only `sales_lines_wide` reads from `sales_line` (line grain)
- NULL `truck_id` / `location_id` rows are kept (warn-level in Silver), surface as an explicit "Unknown" bucket in aggregates
- Liquid Clustering on `sales_lines_wide` (`CLUSTER BY (truck_id, location_id, order_date, order_currency)`)

### Wide table derivations

- `order_date`, `order_hour`, `order_day_of_week`, `order_year_month`
- `shift_duration_minutes` (parse end - parse start)
- `line_subtotal` (`quantity * unit_price`, sanity-check vs `price`)

### Workflow shape

- `setup → (ingest_full ‖ ingest_incremental) → dlt_integration → dlt_datamart`
- `dlt_integration` depends on both ingest tasks; `dlt_datamart` depends on `dlt_integration`

### AI/BI Dashboard

- Serialized as a Lakeview dashboard JSON checked into the repo
- Deployed via a DAB resource so `databricks bundle deploy` provisions it
- Widgets: revenue trend (line, from `monthly_revenue_by_currency`), top trucks by revenue (bar), top locations by revenue (bar), KPI card (total revenue + total orders)

### AI/BI Genie

- Configured manually post-deploy (Genie spaces don't currently serialize cleanly into DAB)
- Runbook step in `docs/demo_script.md`: create a Genie space against `DATAMART.sales_lines_wide` with example questions

### Deep modules to extract

1. **Rule engine** — pure helper. Takes a `{rule_name: predicate}` dict. Returns `(clean_predicate_string, failed_rules_array_expression)`. Used by both cleansed-table and quarantine-table builders. No Spark dependency in the API.
2. **Type-fix helpers** — pure column-transformation functions: `string_to_decimal`, `int_millis_to_time_string`, `cast_id_columns`. Single responsibility, easy to read and replace.
3. **Bronze CDF reader** — wraps `spark.readStream.option("readChangeFeed", "true").table(...)` with consistent options. One place to evolve if the Bronze read pattern changes.
4. **Wide-table derivation helpers** — pure functions for `order_hour`, `order_day_of_week`, `shift_duration_minutes`, `line_subtotal`. Reused if other Gold tables ever need the same derivations.

## Testing Decisions

**No automated tests in scope for this PRD.**

Rationale: This is fundamentally a demo template. The repo has no existing test infrastructure (no `tests/`, no pytest config, no CI test step). The deep modules are small and easily eyeballed; setup cost would outweigh demo benefit.

If the project grows beyond demo scope, the natural test targets are the four deep modules listed under Implementation Decisions — they're pure functions with simple DataFrame inputs/outputs and would be straightforward to test with a local SparkSession + small fixtures.

## Out of Scope

- **SQL Server source path / Lakeflow Connect** — currently parked. Will need its own follow-up PRD when unparked. Silver's CDF + apply_changes pattern was deliberately chosen so the SQL Server CDC path can plug into the same Silver shape.
- **Master data dimensions** (Customer, Truck, Menu, Location) — depend on SQL Server unparking; without master data, dimension tables would be hollow.
- **Star schema with real dimensions** — premature without master data.
- **SCD2 historisation** — premature without master data + change stream.
- **Test infrastructure** — no pytest setup, no CI tests (per Testing Decisions above).
- **Production deployment automation beyond DAB** — single `dev` target only; `test`/`prod` targets remain placeholders.
- **Multi-tenant / multi-workspace deployment** — single workspace assumption.
- **Delta Sharing on the Gold layer** — possible follow-up demo material but not in this scope.
- **ML feature store integration** — `sales_lines_wide` is feature-store-friendly but the integration itself is out of scope.
- **`demo_showcase/` notebooks** — Time Travel, Audit Logs, Lineage notebooks already exist and are outside this PRD.

## Further Notes

- See `docs/adr/0001-dlt-and-data-quality-belong-in-silver.md` and `docs/adr/0002-silver-reads-bronze-via-cdf-and-apply-changes.md` for the two architecturally significant decisions captured during design.
- Bronze CDF was added in the staging cleanup commit; Silver depends on this being in place. Verify with `SHOW TBLPROPERTIES <bronze_table>` on a deployed dev environment before running Silver for the first time.
- DLT pipelines are defined in `.ipynb` notebooks per the project's notebook-format convention (Databricks platform default since the recent platform update).
- The 4 deep modules listed under Implementation Decisions are good candidates for shared placement under `integration/_lib/` and `datamart/_lib/`, or as Python-only `%run` helpers — exact layout to be decided during issue execution.
