# Demo Script — Tasty Bytes KRM Lakehouse

**Audience:** Pre-sales engineers running live customer demos.  
**Duration:** 60–90 minutes end-to-end; each segment can stand alone.  
**Prerequisites:** Layer-1 admin setup complete (see `docs/prerequisites.md`). A Databricks workspace with Unity Catalog, a running SQL warehouse, and the repo checked out in Databricks Repos.

---

## How to use this document

Work through segments 1–13 in order for a full end-to-end demo. Each segment includes:

- **Setup** — what must be true before you start the segment.
- **Steps** — exact UI clicks and SQL to copy-paste.
- **Expected observation** — what success (or deliberate failure) looks like.
- **Talking points** — 1–2 sentences you can say verbatim.

You do not need to refer back to the PRD during the demo.

---

## Pipeline architecture at a glance

```
databricks bundle deploy
        ↓
  [Workflow: demo-end-to-end-workflow]
  setup
    ├─→ ingest_azurestorage_full         (STAGING_AZURESTORAGE — mode=full)
    └─→ ingest_azurestorage_incremental  (STAGING_AZURESTORAGE — mode=incremental)
             ↓
         dlt_integration  (Integration DLT — INTEGRATION schema: DW_/DWQ_/DWH_)
              ├──────────────┐
              ↓              ↓
          dlt_datamart    apply_views  (DIM_<NAAM> plain UC views over DWH_)
          (FCT_ MVs in DATAMART)

  [Dashboard] tasty_bytes_sales   (auto-deployed via bundle)
  [Genie]     tasty_bytes_genie   (post-deploy manual setup — segment 10)
```

The layer vocabulary used throughout this demo follows KRM:

- **Staging** (Ingest layer, `STAGING_AZURESTORAGE`) — raw Auto Loader landings as `STG_<TABEL>` with `SA_*` admin columns.
- **Integration** (Combine layer, `INTEGRATION`) — cleansed history (`DW_<TABEL>`), quarantine (`DWQ_<TABEL>`), and consumer projection views (`DWH_<TABEL>`).
- **Datamart** (Publish layer, `DATAMART`) — fact MVs (`FCT_<NAAM>`) plus plain UC dimension views (`DIM_<NAAM>`).

---

## Segment 1 — Bundle deployment

### Setup

- DAB bundle configured (`databricks.yml`, `resources/`).
- Databricks CLI authenticated: `databricks auth login`.
- Azure Storage parquet files present in the External Volume.

### Steps

1. Open a terminal and run:

   ```bash
   databricks bundle deploy -t dev
   ```

2. After deploy completes, navigate to **Workflows** in the Databricks sidebar.
3. Find the workflow named **`demo-end-to-end-workflow`** and click **Run now**.
4. Watch the DAG view. You will see five tasks light up in order:

   ```
   setup  →  ingest_azurestorage_full   (parallel)
          →  ingest_azurestorage_incremental
                    ↓
              dlt_integration
                    ├──────────────┐
                    ↓              ↓
                dlt_datamart    apply_views
   ```

5. Wait for all tasks to show a green checkmark.

### Expected observation

- All tasks succeed (green).
- `setup` finishes first; the two ingest tasks run in parallel; `dlt_integration` starts after both ingest tasks complete; `dlt_datamart` and `apply_views` start in parallel after `dlt_integration` completes.
- The AI/BI Dashboard `tasty_bytes_sales` is visible in the workspace under **Dashboards**.

### Talking points

> "One command deploys the entire pipeline — the Workflow, both DLT pipelines, the dim views notebook, and the dashboard — as versioned infrastructure. Nothing is configured by hand in the UI."

---

## Segment 2 — Bootstrap idempotency

### Setup

- Segment 1 complete (workflow has run at least once).

### Steps

1. Open the **`demo-end-to-end-workflow`** and click **Run now** again to trigger a second full run.
2. After the run completes, open a SQL editor (Databricks SQL or a notebook) and run:

   ```sql
   SELECT target_table, COUNT(*) AS row_count
   FROM   DEMO.CONFIG.pipeline_sources
   GROUP BY target_table
   ORDER BY target_table;
   ```

3. Compare with the row count from the first run.

4. Also check the staging table counts:

   ```sql
   SELECT COUNT(*) AS order_header_rows FROM DEMO.STAGING_AZURESTORAGE.STG_ORDER_HEADER;
   SELECT COUNT(*) AS order_detail_rows  FROM DEMO.STAGING_AZURESTORAGE.STG_ORDER_DETAIL;
   ```

### Expected observation

- The control table still contains exactly 2 rows (`order_detail`, `order_header`) — no duplicates.
- The staging table row counts are identical to the first run (full-load mode overwrites, not appends).
- The `setup` notebook used a `MERGE` statement, so re-running it is a no-op for pre-existing seed rows.

### Talking points

> "The setup notebook is idempotent — you can run it in any CI/CD pipeline without fear of duplicates or side effects. This is the contract for every notebook in this demo."

---

## Segment 3 — Metadata-driven full → incremental switch (staging)

### Setup

- Segment 1 complete. At least one full-load run has completed.

### Steps

**Step A — Perform the live mode switch:**

1. Open a SQL editor and run:

   ```sql
   UPDATE DEMO.CONFIG.pipeline_sources
   SET    load_type = 'incremental'
   WHERE  source_system = 'azurestorage'
   AND    target_table  = 'order_header';
   ```

2. Verify the change:

   ```sql
   SELECT target_table, load_type
   FROM   DEMO.CONFIG.pipeline_sources
   ORDER BY target_table;
   ```

   Expected: `order_header` now shows `incremental`; `order_detail` still shows `full`.

**Step B — Trigger only the incremental ingest task:**

3. Open the **`demo-end-to-end-workflow`** and click the three-dot menu on the `ingest_azurestorage_incremental` task.
4. Click **Run task** (runs only this task, not the full workflow).
5. Observe that the task completes without loading any new data (Auto Loader checkpoint is absent — it needs initialization).

**Step C — First incremental run after switch (with reset):**

6. Open `staging/02_ingest_azurestorage.ipynb` in the Databricks workspace.
7. In the widget bar at the top, set:
   - `mode` = `incremental`
   - `reset` = `true`
   - `catalog` = `DEMO`
8. Click **Run all**.
9. Observe the output: the notebook drops `order_header`, deletes its checkpoint folder, and re-ingests all source files from scratch.
10. Set `reset` back to `false` for subsequent runs.

**Step D — Confirm behaviour:**

11. Run the full workflow again (Run now). Observe that:
    - `ingest_azurestorage_full` processes `order_detail` (still `full`).
    - `ingest_azurestorage_incremental` processes `order_header` (now `incremental`) via Auto Loader.

### Expected observation

- After the `UPDATE`, re-running the workflow changes which mode each table uses — no code change was made.
- The `reset=true` run clears the Auto Loader checkpoint and avoids duplicating rows from the previous full load.
- `ingest_azurestorage_full` silently skips `order_header` (no rows with `load_type='full'` match).
- `ingest_azurestorage_incremental` silently skips `order_detail` (no rows with `load_type='incremental'` match).

### Talking points

> "One SQL UPDATE changes the pipeline's behaviour from full-replace to change-data-capture — no code deployment, no cluster restart, no pipeline rebuild. The control table is the single source of truth for pipeline configuration."

---

## Segment 4 — Change Data Feed propagation (staging → integration)

### Setup

- Segment 1 complete. Integration tables (`INTEGRATION.DW_ORDER_HEADER`, `INTEGRATION.DW_ORDER_DETAIL`) exist and contain data; consumer views (`DWH_ORDER_HEADER`, `DWH_ORDER_DETAIL`) project them.

### Steps

**Step A — Confirm CDF is enabled on staging:**

1. Run in a SQL editor:

   ```sql
   SHOW TBLPROPERTIES DEMO.STAGING_AZURESTORAGE.STG_ORDER_HEADER;
   ```

   Look for the row: `delta.enableChangeDataFeed = true`.

**Step B — Make a small staging change:**

2. Identify an existing order row:

   ```sql
   SELECT order_id, order_total
   FROM   DEMO.STAGING_AZURESTORAGE.STG_ORDER_HEADER
   LIMIT  5;
   ```

3. Update one row (copy the `order_id` value from the result above):

   ```sql
   UPDATE DEMO.STAGING_AZURESTORAGE.STG_ORDER_HEADER
   SET    order_total = order_total + 1.00
   WHERE  order_id = <paste_order_id_here>;
   ```

**Step C — Run integration:**

4. Trigger the `dlt_integration` pipeline task from the Workflow UI (or run the full workflow).
5. After it completes, check the integration history table directly to see both versions:

   ```sql
   SELECT order_id, order_total, WA_FROMDATE, WA_UNTODATE, WA_ISCURR
   FROM   DEMO.INTEGRATION.DW_ORDER_HEADER
   WHERE  order_id = <same_order_id>
   ORDER  BY WA_FROMDATE;
   ```

6. Then check the projection view (only the current row per BK):

   ```sql
   SELECT order_id, order_total
   FROM   DEMO.INTEGRATION.DWH_ORDER_HEADER
   WHERE  order_id = <same_order_id>;
   ```

**Step D — Confirm propagation:**

7. The updated `order_total` in staging has propagated to integration, with the previous version preserved in `DW_ORDER_HEADER` (closed off via `WA_UNTODATE`) and the new version current (`WA_ISCURR = 1`). The projection `DWH_ORDER_HEADER` shows only the new value.

8. To see the CDF events directly on staging:

   ```sql
   SELECT _change_type, _commit_version, order_id, order_total
   FROM   table_changes('DEMO.STAGING_AZURESTORAGE.STG_ORDER_HEADER', 1)
   ORDER  BY _commit_version DESC
   LIMIT  10;
   ```

### Expected observation

- `SHOW TBLPROPERTIES` shows `delta.enableChangeDataFeed = true` on the staging table.
- `DW_ORDER_HEADER` now contains two rows for that `order_id`: the prior version with a populated `WA_UNTODATE` and `WA_ISCURR = 0`, plus the new current version with `WA_UNTODATE = '9999-12-31'` and `WA_ISCURR = 1`.
- `DWH_ORDER_HEADER` (consumer view) shows exactly one row per `order_id` — the current version — with the new `order_total` value.
- `table_changes()` shows a `update_preimage` event (old value) followed by an `update_postimage` event (new value) on the staging table.

### Talking points

> "Change Data Feed is enabled on every staging table automatically. Integration reads the change stream via `FLOW AUTO CDC ... STORED AS SCD TYPE 2` — every change becomes a new row in `DW_<TABEL>`, with `WA_FROMDATE`/`WA_UNTODATE` validity periods. The `DWH_` projection view exposes just the current version for consumers, while analysts can drop into `DW_` for full history."

---

## Segment 5 — DLT Expectations + Quarantine (integration)

### Setup

- Segment 1 complete. Integration DLT pipeline has run at least once.

### Steps

**Step A — Open the integration DLT graph:**

1. Navigate to **Workflows** → **Delta Live Tables** in the sidebar.
2. Open the pipeline named **`demo-integration`**.
3. View the graph. For each entity you should see four nodes:
   - `<entity>_tagged` (Materialised View — computes `failed_rules ARRAY<STRING>` per CDF event, carries `EXPECT (NOT array_contains(failed_rules, '<rule>'))` constraints)
   - `DW_<TABEL>` (Streaming Table — cleansed history, populated via `FLOW AUTO CDC ... STORED AS SCD TYPE 2` on `WHERE size(failed_rules) = 0`)
   - `DWQ_<TABEL>` (Streaming Table — append-only quarantine, populated on `WHERE size(failed_rules) > 0`)
   - `DWH_<TABEL>` (Materialised View — projection over `DW_<TABEL>`, renames `__START_AT`/`__END_AT` to `WA_FROMDATE`/`WA_UNTODATE`, derives `WA_ISCURR`, computes `WKP_<TABEL>`/`WKR_<TABEL>` surrogates)

   Plus the integration view `SALES_LINE` (Materialised View, joining `DWH_ORDER_HEADER` and `DWH_ORDER_DETAIL`).
4. Click the `order_header_tagged` node. The sidebar shows Expectations metrics: violations per rule, sourced from the `EXPECT (NOT array_contains(failed_rules, '<rule>'))` declarations.

**Step B — Inject a malformed row (drop-rule violation):**

5. Insert a staging row that violates the `order_total_non_negative` drop rule:

   ```sql
   INSERT INTO DEMO.STAGING_AZURESTORAGE.STG_ORDER_HEADER
   (order_id, truck_id, location_id, customer_id, discount_id, shift_id,
    shift_start_time, shift_end_time, order_channel, order_ts, served_ts,
    order_currency, order_amount, order_tax_amount, order_discount_amount,
    order_total,
    SA_CRUDDTS, SA_SRC, SA_RUNID)
   VALUES
   (9999999, 1, 1, 1, NULL, 1,
    28800000, 57600000, 'Online', current_timestamp(), current_timestamp()::string,
    'USD', 100.00, '5.00', '0.00', -1.00,
    current_timestamp(), 'azurestorage', 'demo-manual');
   ```

6. Trigger the `dlt_integration` pipeline task.

**Step C — Confirm quarantine:**

7. After the pipeline completes, run:

   ```sql
   SELECT order_id, order_total, failed_rules
   FROM   DEMO.INTEGRATION.DWQ_ORDER_HEADER
   ORDER BY order_id DESC
   LIMIT  10;
   ```

8. Confirm the injected row (order_id 9999999) is present with `order_total = -1.00` and `failed_rules` containing `'order_total_non_negative'`.

9. Query by specific rule:

   ```sql
   SELECT order_id, order_total, failed_rules
   FROM   DEMO.INTEGRATION.DWQ_ORDER_HEADER
   WHERE  array_contains(failed_rules, 'order_total_non_negative');
   ```

10. Confirm the injected row is **absent** from the cleansed history (and therefore from the projection):

    ```sql
    SELECT COUNT(*) FROM DEMO.INTEGRATION.DW_ORDER_HEADER  WHERE order_id = 9999999;
    SELECT COUNT(*) FROM DEMO.INTEGRATION.DWH_ORDER_HEADER WHERE order_id = 9999999;
    ```

    Expected: 0 rows in each.

**Step D — Demonstrate a fail-rule:**

11. Insert a row with `order_id = NULL`:

    ```sql
    INSERT INTO DEMO.STAGING_AZURESTORAGE.STG_ORDER_HEADER
    (order_id, order_total, order_ts, customer_id, order_currency,
     SA_CRUDDTS, SA_SRC, SA_RUNID)
    VALUES
    (NULL, 50.00, current_timestamp(), 1, 'USD',
     current_timestamp(), 'azurestorage', 'demo-fail');
    ```

12. Trigger the `dlt_integration` pipeline task.
13. Observe that the pipeline **halts** with a fail-expectation error. Click on the `order_header_tagged` node to see the violation.

    > **Cleanup:** Delete the NULL row from staging and re-run the pipeline to restore normal operation:
    >
    > ```sql
    > DELETE FROM DEMO.STAGING_AZURESTORAGE.STG_ORDER_HEADER WHERE order_id IS NULL;
    > ```

### Expected observation

- The DLT graph shows the four-node-per-entity layout with green/orange badges.
- The negative `order_total` row lands in `DWQ_ORDER_HEADER` with `failed_rules = ['order_total_non_negative']`.
- The same row is absent from `DW_ORDER_HEADER` and `DWH_ORDER_HEADER`.
- The `array_contains` query returns exactly the injected row.
- The `NULL order_id` injection halts the entire pipeline (fail-level expectation).

### Talking points

> "Bad rows don't disappear — they land in a paired `DWQ_<TABEL>` quarantine table with a `failed_rules ARRAY<STRING>` column that tells analysts exactly which rules failed. The rule logic appears exactly once, in the upstream tagged MV's `CASE WHEN` expressions; the cleansed `DW_<TABEL>` is always trust-worthy and the quarantine table is always inspectable via `array_contains(failed_rules, '<rule>')`."

---

## Segment 6 — Integrated business view (`SALES_LINE`)

### Setup

- Segment 1 complete. Both `INTEGRATION.DWH_ORDER_HEADER` and `INTEGRATION.DWH_ORDER_DETAIL` contain data.

### Steps

**Step A — Inspect the integrated view:**

1. Run in a SQL editor:

   ```sql
   SELECT *
   FROM   DEMO.INTEGRATION.SALES_LINE
   LIMIT  10;
   ```

2. Observe that each row contains columns from both `DWH_ORDER_HEADER` (truck_id, order_total, order_ts, …) and `DWH_ORDER_DETAIL` (menu_item_id, quantity, unit_price, price, …) joined on `order_id`.

3. Check the row count versus the staging detail table:

   ```sql
   SELECT COUNT(*) AS sales_line_rows  FROM DEMO.INTEGRATION.SALES_LINE;
   SELECT COUNT(*) AS order_detail_rows FROM DEMO.STAGING_AZURESTORAGE.STG_ORDER_DETAIL;
   ```

   The counts should be close (or equal for a clean dataset — quarantined detail rows will be absent from `SALES_LINE`).

**Step B — Demonstrate propagation through the SCD2 chain:**

4. Correct an `order_total` in staging (the canonical source — integration recomputes from CDF):

   ```sql
   -- Find a row to update
   SELECT order_id, order_total FROM DEMO.STAGING_AZURESTORAGE.STG_ORDER_HEADER LIMIT 5;

   -- Make the correction (copy an order_id from above)
   UPDATE DEMO.STAGING_AZURESTORAGE.STG_ORDER_HEADER
   SET    order_total = order_total + 10.00
   WHERE  order_id = <paste_order_id_here>;
   ```

5. Re-trigger the `dlt_integration` pipeline (or the full workflow).

6. After the run, verify the `SALES_LINE` view reflects the correction:

   ```sql
   SELECT sl.order_id, sl.order_total, sl.menu_item_id, sl.quantity
   FROM   DEMO.INTEGRATION.SALES_LINE sl
   WHERE  sl.order_id = <same_order_id>;
   ```

### Expected observation

- `SALES_LINE` rows contain a full denormalised view of each order line with all header attributes from the latest current version.
- After a header correction, every `SALES_LINE` row for that `order_id` reflects the updated `order_total` — the projection chain `DW_ → DWH_ → SALES_LINE` recomputes on each pipeline run.
- Quarantined rows are absent: a row in `DWQ_ORDER_DETAIL` does not appear in `SALES_LINE`.

### Talking points

> "`SALES_LINE` is the integrated enterprise view — one row per order line, all header attributes denormalised over the current `DWH_` projections. Every pipeline run refreshes it, so a correction in staging propagates through `DW_` (versioned history) into `DWH_` (current projection) and on to `SALES_LINE` automatically."

---

## Segment 7 — Datamart fact MVs (`FCT_ORDER`)

### Setup

- Segment 1 complete. datamart DLT pipeline has run at least once. `DATAMART` schema contains data.

### Steps

**Step A — Query the order-grain fact:**

1. Run:

   ```sql
   SELECT MK_DATE, MK_TRUCK, MK_LOCATION, MK_CURRENCY, MK_ORDER_CHANNEL,
          order_id, order_total
   FROM   DEMO.DATAMART.FCT_ORDER
   ORDER  BY MK_DATE DESC, order_total DESC
   LIMIT  20;
   ```

2. Observe that the FK columns (`MK_DATE`, `MK_TRUCK`, `MK_LOCATION`, …) are integer surrogates — `MK_DATE` is `yyyymmdd` (per ADR-0018), the others are IDENTITY-generated BIGINTs derived from `WKR_<TABEL>` (the root surrogate per BK lineage, per ADR-0012). No SHA2 hashes appear on the fact side.

**Step B — Aggregate by truck:**

3. Run:

   ```sql
   SELECT MK_TRUCK,
          COUNT(*)         AS total_orders,
          SUM(order_total) AS total_revenue,
          AVG(order_total) AS avg_order_value
   FROM   DEMO.DATAMART.FCT_ORDER
   GROUP  BY MK_TRUCK
   ORDER  BY total_revenue DESC
   LIMIT  20;
   ```

4. Point out any row where `MK_TRUCK` is the "Unknown" sentinel — this is the intentional bucket for orders where staging had no truck assignment (per ADR-0007, surfaced rather than silently dropped).

**Step C — Join to `DIM_TRUCK` for human-readable labels:**

5. Run:

   ```sql
   SELECT t.truck_id, t.truck_brand_name,
          COUNT(*)           AS total_orders,
          SUM(f.order_total) AS total_revenue
   FROM   DEMO.DATAMART.FCT_ORDER f
   JOIN   DEMO.DATAMART.DIM_TRUCK t ON f.MK_TRUCK = t.MK_TRUCK
   GROUP  BY t.truck_id, t.truck_brand_name
   ORDER  BY total_revenue DESC
   LIMIT  10;
   ```

**Step D — Monthly trend via `DIM_DATE`:**

6. `DIM_DATE` is a generated calendar view (per ADR-0018) keyed by `MK_DATE = yyyymmdd`. Run:

   ```sql
   SELECT d.year_month_start, f.MK_CURRENCY,
          COUNT(*)           AS total_orders,
          SUM(f.order_total) AS total_revenue,
          AVG(f.order_total) AS avg_order_value
   FROM   DEMO.DATAMART.FCT_ORDER f
   JOIN   DEMO.DATAMART.DIM_DATE  d ON f.MK_DATE = d.MK_DATE
   GROUP  BY d.year_month_start, f.MK_CURRENCY
   ORDER  BY d.year_month_start DESC, total_revenue DESC;
   ```

### Expected observation

- `FCT_ORDER` exposes IDENTITY-generated `MK_<NAAM>` surrogates as FK columns (BIGINT) plus `MK_DATE` as a `yyyymmdd` INT.
- Joining `MK_TRUCK` to `DIM_TRUCK.MK_TRUCK` resolves to the latest version's business attributes (SCD1 dim per ADR-0012).
- Joining `MK_DATE` to `DIM_DATE.MK_DATE` resolves to a generated calendar row — covering exactly the date range observed in the fact.
- If an "Unknown" surrogate row exists (BK was NULL upstream), it appears as a single row in the aggregate rather than being silently discarded.

### Talking points

> "The fact reads from `DWH_<TABEL>` directly and projects `WKR_<TABEL>` as the `MK_<TABEL>` FK — the root surrogate, stable across all future updates and deletes of an entity. No SHA2 hash on the fact side; the consumer joins integer-to-integer between fact and dim. The 'Unknown' bucket surfaces attribution issues from staging in the aggregate rather than swallowing them silently."

---

## Segment 8 — Line-grain fact + Liquid Clustering (`FCT_SALES_LINE`)

### Setup

- Segment 1 complete. `DATAMART.FCT_SALES_LINE` exists and contains data.

### Steps

**Step A — Inspect the line-grain fact:**

1. Run:

   ```sql
   SELECT order_detail_id, order_id, line_number,
          MK_TRUCK, MK_LOCATION, MK_DATE, MK_CURRENCY,
          order_ts,
          quantity, unit_price, price, line_subtotal
   FROM   DEMO.DATAMART.FCT_SALES_LINE
   LIMIT  10;
   ```

2. Point out the columns: degenerate dims (`order_id`, `order_detail_id`, `line_number`), event timestamps (`order_ts`, `served_ts`) denormalised from the header, IDENTITY-generated `MK_<NAAM>` FK surrogates, measures (`quantity`, `unit_price`, `price`), and the derived `line_subtotal = CAST(quantity * unit_price AS DECIMAL(38,4))`.

**Step B — Confirm Liquid Clustering:**

3. Run:

   ```sql
   SHOW TBLPROPERTIES DEMO.DATAMART.FCT_SALES_LINE;
   ```

4. Find the row: `clusteringColumns = [["mk_truck"],["mk_location"],["mk_date"],["mk_currency"]]` (the table declaration is `CLUSTER BY (MK_TRUCK, MK_LOCATION, MK_DATE, MK_CURRENCY)`).

5. Demonstrate a clustered query:

   ```sql
   SELECT MK_DATE, MK_TRUCK, MK_CURRENCY,
          COUNT(*) AS line_count, SUM(line_subtotal) AS subtotal
   FROM   DEMO.DATAMART.FCT_SALES_LINE
   WHERE  MK_TRUCK = (SELECT MK_TRUCK FROM DEMO.DATAMART.DIM_TRUCK WHERE truck_id = 1)
   AND    MK_DATE >= 20240101
   GROUP  BY MK_DATE, MK_TRUCK, MK_CURRENCY
   ORDER  BY MK_DATE;
   ```

**Step C — Demonstrate time-of-day analysis (Genie-ready) via `DIM_DATE`:**

6. Run:

   ```sql
   SELECT HOUR(f.order_ts)      AS order_hour,
          d.day_name             AS order_day_of_week,
          COUNT(*)               AS line_count,
          ROUND(AVG(f.line_subtotal), 2) AS avg_line_value
   FROM   DEMO.DATAMART.FCT_SALES_LINE f
   JOIN   DEMO.DATAMART.DIM_DATE       d ON f.MK_DATE = d.MK_DATE
   GROUP  BY HOUR(f.order_ts), d.day_name, d.day_of_week
   ORDER  BY d.day_of_week, order_hour;
   ```

### Expected observation

- `SHOW TBLPROPERTIES` shows `clusteringColumns` with the four declared keys: `MK_TRUCK`, `MK_LOCATION`, `MK_DATE`, `MK_CURRENCY`.
- The fact carries `order_ts`/`served_ts` denormalised from the header so time-of-day analyses (HOUR/DAYOFWEEK) work directly on the fact.
- `line_subtotal = quantity * unit_price` — customers can spot `price` vs `line_subtotal` discrepancies.
- The clustered query runs faster on repeat execution as Databricks applies the clustering automatically.

### Talking points

> "`FCT_SALES_LINE` is the line-grain wide fact — heaviest table in the model, materialised to absorb the join and to benefit from Liquid Clustering. We declare four cluster keys (`MK_TRUCK`, `MK_LOCATION`, `MK_DATE`, `MK_CURRENCY`); Databricks automatically reorganises the data as query patterns evolve. No ALTER TABLE, no partition strategy debates, no data migration."

---

## Segment 9 — AI/BI Dashboard

### Setup

- Segment 1 complete (`databricks bundle deploy` has run). `DATAMART` tables contain data.

### Steps

1. Navigate to **Dashboards** in the Databricks sidebar.
2. Find the dashboard named **`tasty_bytes_sales`** and open it.
3. Walk through the four widgets:

   | Widget | Source table | What it shows |
   |---|---|---|
   | Revenue trend (line chart) | `DATAMART.FCT_ORDER` + `DIM_DATE` + `DIM_CURRENCY` | Monthly revenue over time, one line per currency |
   | Top trucks by revenue (bar chart) | `DATAMART.FCT_ORDER` + `DIM_TRUCK` | Top 10 trucks ranked by total revenue |
   | Top locations by revenue (bar chart) | `DATAMART.FCT_ORDER` + `DIM_LOCATION` | Top 10 locations ranked by total revenue |
   | KPI card | `DATAMART.FCT_ORDER` | Total revenue + total orders across all time |

4. Click the **Refresh** button to pull the latest data from the datamart tables.

5. Demonstrate the date filter (if present in the dashboard): adjust the date range and observe that all widgets update simultaneously.

6. Optionally, click **Edit** to show that the dashboard definition is backed by the checked-in file `dashboards/tasty_bytes_sales.lvdash.json` — it is versioned in Git and deployed automatically by the bundle.

### Expected observation

- All four widgets render with data.
- Refreshing the dashboard queries the datamart tables directly — no intermediate cache or import step.
- The dashboard was deployed automatically by `databricks bundle deploy` — no manual creation in the UI was needed.

### Talking points

> "The dashboard is code — it lives in the Git repo, deploys with the bundle, and is always in sync with the pipeline schema. Widgets join `FCT_<NAAM>` to `DIM_<NAAM>` views — the dim views are plain UC views over `DWH_`, so every dashboard refresh sees current dim attributes without an extra materialisation step."

---

## Segment 10 — AI/BI Genie space (post-deploy setup + live demo)

### 10a — First-time setup (run once after bundle deploy)

**Prerequisites:** `dlt_datamart` pipeline has run at least once and `DATAMART.FCT_SALES_LINE` contains data. Per the PRD, Genie configuration is a manual post-deploy step.

1. Navigate to **AI/BI** → **Genie** in the Databricks workspace sidebar.
2. Click **New Genie space** (or the **+** button).
3. Enter the name: **`tasty_bytes_genie`**.
4. Under **Tables**, click **Add table** and add the following from catalog `DEMO`, schema `DATAMART`:
   - `FCT_SALES_LINE` (the primary fact)
   - `DIM_TRUCK`, `DIM_LOCATION`, `DIM_DATE`, `DIM_CURRENCY`, `DIM_MENU_ITEM` (so Genie can resolve `MK_*` surrogates to human-readable labels)
5. Click **Save**.
6. Under **Example questions**, add the following four questions (copy-paste each):
   - `Welke truck had vorige week de meeste revenue?`
   - `Vergelijk revenue per uur van de dag tussen truck 1 en truck 2`
   - `Wat is de gemiddelde order value per locatie deze maand?`
   - `Welke menu_item_id wordt het meest verkocht op zondag?`
7. (Optional) Configure column-level metadata so Genie understands the surrogate-to-attribute joins:
   - Click on `FCT_SALES_LINE` in the Genie space editor.
   - Find column `MK_TRUCK` and add the description: `IDENTITY-generated surrogate FK to DIM_TRUCK. Join on DIM_TRUCK.MK_TRUCK to resolve truck_brand_name, truck_type, etc.`
   - Find column `MK_DATE` and add: `Date surrogate in yyyymmdd integer form (e.g. 20240315). Join on DIM_DATE.MK_DATE for calendar attributes.`
   - Find column `order_ts` and add: `Original event timestamp. Use HOUR(order_ts) for time-of-day analysis.`
   - Click **Save**.
8. The Genie space is now ready. Proceed to 10b for the live demo.

### 10b — Live demo

**Prerequisites:** Genie space `tasty_bytes_genie` exists (10a complete).

1. Open the **`tasty_bytes_genie`** Genie space.
2. In the chat input, type one of the example questions:

   ```
   Welke truck had vorige week de meeste revenue?
   ```

3. Watch Genie generate a SQL query, execute it, and display the result as a table or chart.
4. Click **Show SQL** to reveal the generated query. Observe that it filters on a date range for "vorige week" (typically via `DIM_DATE.full_date` or `MK_DATE`), aggregates `order_total` (or `line_subtotal`) per `MK_TRUCK`, and joins `DIM_TRUCK` to display `truck_brand_name`.
5. Ask a follow-up question:

   ```
   Vergelijk revenue per uur van de dag tussen truck 1 en truck 2
   ```

6. Observe that Genie uses `HOUR(order_ts)` on `FCT_SALES_LINE` and joins `DIM_TRUCK` to filter by `truck_id`. This works because `order_ts` is denormalised onto the line-grain fact.

### Expected observation

- Genie answers the question in natural language and shows a SQL query.
- The generated SQL references `DEMO.DATAMART.FCT_SALES_LINE` plus the registered `DIM_<NAAM>` views.
- Joining `MK_<NAAM>` surrogates to dim views lets Genie answer in business terms (truck brand name, location city, day name) rather than raw IDs.

### Talking points

> "Genie answers business questions in natural language. We expose the line-grain fact `FCT_SALES_LINE` together with the `DIM_<NAAM>` views — so questions get answered in business attributes (brand, day name, currency code) even though the fact carries integer surrogates. The denormalised `order_ts` on the fact makes time-of-day questions answerable without any extra calculation."

---

## Segment 11 — Time Travel

### Setup

- Segment 3 complete (at least one `UPDATE` to the control table has been performed, creating a second Delta version).

### Steps

1. Open `demo_showcase/delta_time_travel.ipynb` in the Databricks workspace.
2. Confirm the `catalog` widget is set to `DEMO`.
3. Click **Run all**.
4. Walk through the four sections of the notebook:

**Section 1 — Current state:**

The notebook displays the current content of `DEMO.CONFIG.pipeline_sources` — you should see the `load_type = 'incremental'` change made in segment 3.

**Section 2 — VERSION AS OF:**

```sql
SELECT *
FROM   DEMO.CONFIG.pipeline_sources VERSION AS OF 1
ORDER BY target_table;
```

This returns the state after the first write (seed rows, both `load_type = 'full'`).

**Section 3 — TIMESTAMP AS OF:**

The notebook dynamically looks up the timestamp of the earliest version and queries:

```sql
SELECT *
FROM   DEMO.CONFIG.pipeline_sources TIMESTAMP AS OF '<earliest_timestamp>'
ORDER BY target_table;
```

This shows the state of the control table at a specific point in time.

**Section 4 — DESCRIBE HISTORY:**

```sql
DESCRIBE HISTORY DEMO.CONFIG.pipeline_sources;
```

Shows every version: version number, timestamp, operation type (`MERGE`, `UPDATE`, `WRITE`), and the username of whoever ran the operation.

### Expected observation

- `VERSION AS OF 1` returns 2 rows with `load_type = 'full'` for both tables.
- The most recent version shows `order_header` with `load_type = 'incremental'`.
- `DESCRIBE HISTORY` shows at least 3 versions: initial `CREATE`, the `MERGE` (seed rows), and the `UPDATE` (mode switch).

### Talking points

> "Delta Lake's transaction log is a built-in time machine. No back-up or restore procedure required — every version of the control table is queryable with a single SQL clause. This is how compliance teams reconstruct data state for a specific reporting date — complementing the explicit SCD2 history we already keep in `DW_<TABEL>` for the data itself."

---

## Segment 12 — Audit Logs

### Setup

- System Tables must be enabled on the metastore (`system.access.audit` available). If not, go to **Account Console → Metastore → System schemas** and enable `access`. Wait 10–30 minutes for records to appear.

### Steps

1. Open `demo_showcase/audit_logs.ipynb` in the Databricks workspace.
2. Confirm widgets: `catalog = DEMO`, `lookback_days = 30`.
3. Click **Run all**.
4. Walk through the five sections of the notebook:

**Section 1 — Preflight check:**

The notebook checks whether `system.access.audit` is reachable and provides a clear error message if System Tables are not enabled.

**Section 2 — Recent activity on `DEMO.*`:**

```sql
SELECT event_time, user_identity.email, action_name, request_params.schema_name,
       request_params.table_name
FROM   system.access.audit
WHERE  event_time >= CURRENT_TIMESTAMP() - INTERVAL 30 DAYS
AND    LOWER(request_params.catalog_name) = 'demo'
ORDER  BY event_time DESC
LIMIT  100;
```

Point out: `createTable`, `writeTable`, `getTable` events — each triggered by a pipeline run.

**Section 3 — Control table audit:**

Shows only events on `CONFIG.pipeline_sources`: the `createTable` from setup, the `MERGE` from seed-loading, and the `UPDATE` from the mode switch in segment 3.

**Section 4 — Pipeline run events:**

Filters on `service_name IN ('jobs', 'pipelines')` to show `runStart`, `runSucceeded`, `pipelineStarted`, `pipelineCompleted` events.

**Section 5 — Summary:**

Aggregated view: which user performed which action how many times. Useful for compliance reports.

### Expected observation

- All five notebook sections complete without error.
- The control table audit section shows at least three events: `CREATE`, `MERGE`, `UPDATE`.
- Pipeline events section shows job runs triggered in segment 1 (and any subsequent runs).
- The `response.status_code` column is `200` for all successful operations.

### Talking points

> "The Unity Catalog audit log records every data access and modification — automatically, without any instrumentation. Combined with Delta Time Travel and the SCD2 history in `DW_<TABEL>`, we have the 'who' (audit log), the 'when' (`WA_FROMDATE`/`WA_UNTODATE`), and the 'what' (Delta version history) for every change to every table."

---

## Segment 13 — Lineage

### Setup

- System Tables must be enabled (`system.lineage.table_lineage` available). Enable the `lineage` system schema in **Account Console → Metastore → System schemas** if not already done. At least one full end-to-end pipeline run must have completed.

### Steps

1. Open `demo_showcase/lineage.ipynb` in the Databricks workspace.
2. Confirm the `catalog` widget is set to `DEMO`.
3. Click **Run all**.
4. Walk through the notebook sections:

**Section 1 — Preflight:**

Confirms `system.lineage.table_lineage` is accessible.

**Section 2 — Staging lineage:**

Visual step-by-step in Catalog Explorer:

1. Navigate to **Catalog** → `DEMO` → `STAGING_AZURESTORAGE` → `STG_ORDER_HEADER` → **Lineage** tab.
2. The upstream node is the parquet volume path `/Volumes/demo/staging_azurestorage/parquet`.

Programmatic check:

```sql
SELECT source_table_full_name, target_table_full_name, entity_type
FROM   system.lineage.table_lineage
WHERE  LOWER(target_table_full_name) = 'demo.staging_azurestorage.stg_order_header'
ORDER  BY created_at DESC;
```

**Section 3 — Integration lineage:**

Visual:

1. Navigate to `DEMO` → `INTEGRATION` → `SALES_LINE` → **Lineage** tab.
2. Two upstream nodes: `INTEGRATION.DWH_ORDER_HEADER` and `INTEGRATION.DWH_ORDER_DETAIL`.
3. Click the **Column lineage** tab. Select column `order_total`. Trace it back through `DWH_ORDER_HEADER.order_total` → `DW_ORDER_HEADER.order_total` → `STG_ORDER_HEADER.ORDER_TOTAL`.

Programmatic check:

```sql
SELECT source_table_full_name, target_table_full_name
FROM   system.lineage.table_lineage
WHERE  LOWER(target_table_full_name) = 'demo.integration.sales_line';
```

**Section 4 — Datamart lineage (fact):**

Navigate to `DEMO` → `DATAMART` → `FCT_ORDER` → **Lineage** tab. One upstream node: `INTEGRATION.DWH_ORDER_HEADER`.

**Section 5 — Datamart lineage (line-grain fact, full chain):**

Navigate to `DEMO` → `DATAMART` → `FCT_SALES_LINE` → **Lineage** tab. Trace the full chain:

```
parquet files → STG_ORDER_HEADER (staging) → DW_ORDER_HEADER → DWH_ORDER_HEADER
                                                                          ↘
                                                                           SALES_LINE (integration view)
parquet files → STG_ORDER_DETAIL (staging) → DW_ORDER_DETAIL → DWH_ORDER_DETAIL   ↓
                                                                           FCT_SALES_LINE (datamart MV)
```

**Section 6 — Programmatic lineage + impact analysis:**

The notebook runs a recursive CTE to show the full downstream chain from `STAGING_AZURESTORAGE.STG_ORDER_HEADER`:

```sql
-- Which tables are affected if I change STAGING_AZURESTORAGE.STG_ORDER_HEADER?
SELECT DISTINCT target_table_full_name
FROM   system.lineage.table_lineage
WHERE  LOWER(source_table_full_name) = 'demo.staging_azurestorage.stg_order_header';
```

**Section 7 — Governance narrative:**

Summarises the four lineage use cases: impact analysis, debugging, compliance, and automatic catalogisation.

### Expected observation

- The Catalog Explorer Lineage tab shows the chain visually: parquet → staging → integration (DW_ → DWH_) → datamart.
- Column-level lineage traces `order_total` from the datamart fact back to the staging source column.
- The recursive CTE query returns all downstream tables for a given staging table.
- All lineage was captured automatically — no manual registration, API calls, or annotations were made.

### Talking points

> "Unity Catalog captures lineage automatically — from raw parquet files all the way through staging, integration (`DW_` history → `DWH_` projection), and datamart (`FCT_`, `DIM_`). Column-level lineage means you can trace any metric in a dashboard back to its exact source column in the raw data. No separate data catalog product required."

---

## Quick reference — SQL snippets

### Control table

```sql
-- Current state
SELECT * FROM DEMO.CONFIG.pipeline_sources ORDER BY target_table;

-- Mode switch: full → incremental
UPDATE DEMO.CONFIG.pipeline_sources
SET    load_type = 'incremental'
WHERE  source_system = 'azurestorage'
AND    target_table  = 'order_header';
```

### CDF inspection

```sql
-- See recent change events on a staging table
SELECT _change_type, _commit_version, order_id, order_total
FROM   table_changes('DEMO.STAGING_AZURESTORAGE.STG_ORDER_HEADER', 1)
ORDER  BY _commit_version DESC
LIMIT  20;

-- Confirm CDF is enabled
SHOW TBLPROPERTIES DEMO.STAGING_AZURESTORAGE.STG_ORDER_HEADER;
```

### Quarantine triage

```sql
-- All quarantine rows for order_header
SELECT order_id, order_total, failed_rules
FROM   DEMO.INTEGRATION.DWQ_ORDER_HEADER
ORDER  BY order_id DESC
LIMIT  20;

-- Filter by specific failed rule
SELECT order_id, order_total, failed_rules
FROM   DEMO.INTEGRATION.DWQ_ORDER_HEADER
WHERE  array_contains(failed_rules, 'order_total_non_negative');

-- Detail quarantine
SELECT order_detail_id, quantity, failed_rules
FROM   DEMO.INTEGRATION.DWQ_ORDER_DETAIL
WHERE  array_contains(failed_rules, 'quantity_positive');
```

### SCD2 history inspection

```sql
-- All versions of an order, including end-dated rows
SELECT order_id, order_total, WA_FROMDATE, WA_UNTODATE, WA_ISCURR
FROM   DEMO.INTEGRATION.DW_ORDER_HEADER
WHERE  order_id = <order_id>
ORDER  BY WA_FROMDATE;

-- Current view of the same order (one row per BK)
SELECT order_id, order_total, WKP_ORDER_HEADER, WKR_ORDER_HEADER
FROM   DEMO.INTEGRATION.DWH_ORDER_HEADER
WHERE  order_id = <order_id>;
```

### Integration view

```sql
SELECT order_detail_id, order_id, menu_item_id, quantity, order_total, truck_id, order_ts
FROM   DEMO.INTEGRATION.SALES_LINE
LIMIT  10;
```

### Datamart facts + dims

```sql
-- Top trucks by revenue (join fact to SCD1 dim)
SELECT t.truck_id, t.truck_brand_name, SUM(f.order_total) AS revenue
FROM   DEMO.DATAMART.FCT_ORDER f
JOIN   DEMO.DATAMART.DIM_TRUCK t ON f.MK_TRUCK = t.MK_TRUCK
GROUP  BY t.truck_id, t.truck_brand_name
ORDER  BY revenue DESC
LIMIT  10;

-- Monthly trend (join fact to generated DIM_DATE)
SELECT d.year_month_start, f.MK_CURRENCY, SUM(f.order_total) AS revenue
FROM   DEMO.DATAMART.FCT_ORDER f
JOIN   DEMO.DATAMART.DIM_DATE  d ON f.MK_DATE = d.MK_DATE
GROUP  BY d.year_month_start, f.MK_CURRENCY
ORDER  BY d.year_month_start DESC, revenue DESC;

-- Liquid Clustering keys on FCT_SALES_LINE
SHOW TBLPROPERTIES DEMO.DATAMART.FCT_SALES_LINE;
```

### Delta Time Travel

```sql
-- Version history
DESCRIBE HISTORY DEMO.CONFIG.pipeline_sources;

-- Previous version
SELECT * FROM DEMO.CONFIG.pipeline_sources VERSION AS OF 1;

-- At a point in time
SELECT * FROM DEMO.CONFIG.pipeline_sources TIMESTAMP AS OF '2024-06-01 12:00:00';
```

### Lineage (programmatic)

```sql
-- Downstream impact from staging order_header
SELECT DISTINCT target_table_full_name
FROM   system.lineage.table_lineage
WHERE  LOWER(source_table_full_name) = 'demo.staging_azurestorage.stg_order_header';

-- Upstream trace for line-grain fact
SELECT source_table_full_name, target_table_full_name
FROM   system.lineage.table_lineage
WHERE  LOWER(target_table_full_name) = 'demo.datamart.fct_sales_line';
```

---

## Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| `ingest_azurestorage_incremental` task skips all tables | All rows in control table have `load_type='full'` | Segment 3: run the `UPDATE` to switch at least one row to `incremental` |
| Integration pipeline halts with "FAIL expectation violated" | A row with `order_id IS NULL` or `order_detail_id IS NULL` is in staging | Delete the bad row from staging and re-trigger the pipeline |
| `system.access.audit` not found | System Tables not enabled | Account Console → Metastore → System schemas → enable `access` |
| `system.lineage.table_lineage` not found | Lineage System Tables not enabled | Account Console → Metastore → System schemas → enable `lineage` |
| Genie space not showing `FCT_SALES_LINE` | Datamart pipeline has not run | Run the full workflow first; wait for `dlt_datamart` to complete |
| Dashboard shows empty widgets | Datamart tables empty or warehouse not running | Start the SQL warehouse; run the full workflow |
| `table_changes()` returns empty | CDF was not enabled at table creation time | The setup notebook creates staging tables with CDF enabled; if tables were created manually, run `ALTER TABLE ... SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')` |
| `MK_<NAAM>` join to `DIM_<NAAM>` returns no rows for one of the BKs | Dim view hasn't refreshed after a new BK appeared in staging | Re-run the `apply_views` task in the workflow; dim views are projections over `DWH_`, so they refresh quickly |
