# Demo Script — Tasty Bytes Medallion Lakehouse

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

You do not need to refer back to `CONTEXT.md` or the PRD during the demo.

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
         dlt_integration  (Silver DLT — INTEGRATION schema)
              ↓
          dlt_datamart    (Gold DLT — DATAMART schema)

  [Dashboard] tasty_bytes_sales   (auto-deployed via bundle)
  [Genie]     tasty_bytes_genie   (post-deploy manual setup — segment 10)
```

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
4. Watch the DAG view. You will see four tasks light up in order:

   ```
   setup  →  ingest_azurestorage_full   (parallel)
          →  ingest_azurestorage_incremental
                    ↓
              dlt_integration
                    ↓
               dlt_datamart
   ```

5. Wait for all four tasks to show a green checkmark.

### Expected observation

- All four tasks succeed (green).
- `setup` finishes first; the two ingest tasks run in parallel; `dlt_integration` starts after both ingest tasks complete; `dlt_datamart` starts after `dlt_integration` completes.
- The AI/BI Dashboard `tasty_bytes_sales` is visible in the workspace under **Dashboards**.

### Talking points

> "One command deploys the entire pipeline — the Workflow, both DLT pipelines, and the dashboard — as versioned infrastructure. Nothing is configured by hand in the UI."

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

4. Also check the Bronze table counts:

   ```sql
   SELECT COUNT(*) AS order_header_rows FROM DEMO.STAGING_AZURESTORAGE.order_header;
   SELECT COUNT(*) AS order_detail_rows  FROM DEMO.STAGING_AZURESTORAGE.order_detail;
   ```

### Expected observation

- The control table still contains exactly 2 rows (`order_detail`, `order_header`) — no duplicates.
- The Bronze table row counts are identical to the first run (full-load mode overwrites, not appends).
- The `setup` notebook used a `MERGE` statement, so re-running it is a no-op for pre-existing seed rows.

### Talking points

> "The setup notebook is idempotent — you can run it in any CI/CD pipeline without fear of duplicates or side effects. This is the contract for every notebook in this demo."

---

## Segment 3 — Metadata-driven full → incremental switch (Bronze)

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

## Segment 4 — Change Data Feed propagation (Bronze → Silver)

### Setup

- Segment 1 complete. Silver tables (`INTEGRATION.order_header`, `INTEGRATION.order_detail`) exist and contain data.

### Steps

**Step A — Confirm CDF is enabled on Bronze:**

1. Run in a SQL editor:

   ```sql
   SHOW TBLPROPERTIES DEMO.STAGING_AZURESTORAGE.order_header;
   ```

   Look for the row: `delta.enableChangeDataFeed = true`.

**Step B — Make a small Bronze change:**

2. Identify an existing order row:

   ```sql
   SELECT order_id, order_total
   FROM   DEMO.STAGING_AZURESTORAGE.order_header
   LIMIT  5;
   ```

3. Update one row (copy the `order_id` value from the result above):

   ```sql
   UPDATE DEMO.STAGING_AZURESTORAGE.order_header
   SET    order_total = order_total + 1.00
   WHERE  order_id = <paste_order_id_here>;
   ```

**Step C — Run Silver:**

4. Trigger the `dlt_integration` pipeline task from the Workflow UI (or run the full workflow).
5. After it completes, check Silver:

   ```sql
   SELECT order_id, order_total
   FROM   DEMO.INTEGRATION.order_header
   WHERE  order_id = <same_order_id>;
   ```

**Step D — Confirm propagation:**

6. The updated `order_total` in Bronze has propagated to Silver.

7. To see the CDF events directly:

   ```sql
   SELECT _change_type, _commit_version, order_id, order_total
   FROM   table_changes('DEMO.STAGING_AZURESTORAGE.order_header', 1)
   ORDER  BY _commit_version DESC
   LIMIT  10;
   ```

### Expected observation

- `SHOW TBLPROPERTIES` shows `delta.enableChangeDataFeed = true` on the Bronze table.
- After the Silver pipeline runs, the updated `order_total` value is visible in `INTEGRATION.order_header`.
- `table_changes()` shows a `update_preimage` event (old value) followed by an `update_postimage` event (new value).

### Talking points

> "Change Data Feed is enabled on every Bronze table automatically. Silver reads the change stream via `apply_changes` — so a Bronze `UPDATE` or full-table overwrite both propagate cleanly to Silver without duplicating rows or breaking the pipeline."

---

## Segment 5 — DLT Expectations + Quarantine (Silver)

### Setup

- Segment 1 complete. Silver DLT pipeline has run at least once.

### Steps

**Step A — Open the Silver DLT graph:**

1. Navigate to **Workflows** → **Delta Live Tables** in the sidebar.
2. Open the pipeline named **`demo-silver-integration`**.
3. View the graph. You should see five nodes:
   - `order_header` (Streaming Table, cleansed)
   - `order_header_quarantine` (Streaming Table)
   - `order_detail` (Streaming Table, cleansed)
   - `order_detail_quarantine` (Streaming Table)
   - `sales_line` (Materialised View, reading from both cleansed tables)
4. Click the `order_header` node. The sidebar shows Expectations metrics: violations per rule.

**Step B — Inject a malformed row (drop-rule violation):**

5. Insert a Bronze row that violates the `order_total_non_negative` drop rule:

   ```sql
   INSERT INTO DEMO.STAGING_AZURESTORAGE.order_header
   (order_id, truck_id, location_id, customer_id, discount_id, shift_id,
    shift_start_time, shift_end_time, order_channel, order_ts, served_ts,
    order_currency, order_amount, order_tax_amount, order_discount_amount,
    order_total,
    _ingestion_timestamp, _source_system, _source_file, _last_modified, _pipeline_run_id)
   VALUES
   (9999999, 1, 1, 1, NULL, 1,
    28800000, 57600000, 'Online', current_timestamp(), current_timestamp()::string,
    'USD', 100.00, '5.00', '0.00', -1.00,
    current_timestamp(), 'azurestorage', 'manual_inject.parquet', current_timestamp(), 'demo-manual');
   ```

6. Trigger the `dlt_integration` pipeline task.

**Step C — Confirm quarantine:**

7. After the pipeline completes, run:

   ```sql
   SELECT order_id, order_total, failed_rules
   FROM   DEMO.INTEGRATION.order_header_quarantine
   ORDER BY order_id DESC
   LIMIT  10;
   ```

8. Confirm the injected row (order_id 9999999) is present with `order_total = -1.00`.

9. Query by specific rule:

   ```sql
   SELECT order_id, order_total, failed_rules
   FROM   DEMO.INTEGRATION.order_header_quarantine
   WHERE  array_contains(failed_rules, 'order_total_non_negative');
   ```

10. Confirm the injected row is **absent** from the cleansed table:

    ```sql
    SELECT COUNT(*) FROM DEMO.INTEGRATION.order_header WHERE order_id = 9999999;
    ```

    Expected: 0 rows.

**Step D — Demonstrate a fail-rule:**

11. Insert a row with `order_id = NULL`:

    ```sql
    INSERT INTO DEMO.STAGING_AZURESTORAGE.order_header
    (order_id, order_total, order_ts, customer_id, order_currency,
     _ingestion_timestamp, _source_system, _source_file, _last_modified, _pipeline_run_id)
    VALUES
    (NULL, 50.00, current_timestamp(), 1, 'USD',
     current_timestamp(), 'azurestorage', 'fail_inject.parquet', current_timestamp(), 'demo-fail');
    ```

12. Trigger the `dlt_integration` pipeline task.
13. Observe that the pipeline **halts** with a fail-expectation error. Click on the `order_header_bronze_cdf` node to see the violation.

    > **Cleanup:** Delete the NULL row from Bronze and re-run the pipeline to restore normal operation:
    >
    > ```sql
    > DELETE FROM DEMO.STAGING_AZURESTORAGE.order_header WHERE order_id IS NULL;
    > ```

### Expected observation

- The DLT graph shows five nodes with green/orange badges.
- The negative `order_total` row lands in `order_header_quarantine` with `failed_rules = ['order_total_non_negative']`.
- The same row is absent from `INTEGRATION.order_header`.
- The `array_contains` query returns exactly the injected row.
- The `NULL order_id` injection halts the entire pipeline (fail-level expectation).

### Talking points

> "Bad rows don't disappear — they land in a paired quarantine table with a `failed_rules` array that tells analysts exactly which rules failed. The cleansed table is always trust-worthy; the quarantine table is always inspectable."

---

## Segment 6 — Integrated business view (`sales_line`)

### Setup

- Segment 1 complete. Both `INTEGRATION.order_header` and `INTEGRATION.order_detail` contain data.

### Steps

**Step A — Inspect the Materialised View:**

1. Run in a SQL editor:

   ```sql
   SELECT *
   FROM   DEMO.INTEGRATION.sales_line
   LIMIT  10;
   ```

2. Observe that each row contains columns from both `order_header` (truck_id, order_total, order_ts, …) and `order_detail` (menu_item_id, quantity, unit_price, price, …) joined on `order_id`.

3. Check the row count versus the Bronze detail table:

   ```sql
   SELECT COUNT(*) AS sales_line_rows  FROM DEMO.INTEGRATION.sales_line;
   SELECT COUNT(*) AS order_detail_rows FROM DEMO.STAGING_AZURESTORAGE.order_detail;
   ```

   The counts should be close (or equal for a clean dataset — quarantined detail rows will be absent from `sales_line`).

**Step B — Demonstrate MV propagation:**

4. Correct an `order_total` in the cleansed header table (or in Bronze and re-run Silver):

   ```sql
   -- Find a row to update
   SELECT order_id, order_total FROM DEMO.INTEGRATION.order_header LIMIT 5;

   -- Make the correction (copy an order_id from above)
   UPDATE DEMO.INTEGRATION.order_header
   SET    order_total = order_total + 10.00
   WHERE  order_id = <paste_order_id_here>;
   ```

5. Re-trigger the `dlt_integration` pipeline (or the full workflow).

6. After the run, verify the `sales_line` MV reflects the correction:

   ```sql
   SELECT sl.order_id, sl.order_total, sl.menu_item_id, sl.quantity
   FROM   DEMO.INTEGRATION.sales_line sl
   WHERE  sl.order_id = <same_order_id>;
   ```

### Expected observation

- `sales_line` rows contain a full denormalised view of each order line with all header attributes.
- After a header correction, every `sales_line` row for that `order_id` reflects the updated `order_total` — the MV is fully recomputed on each pipeline run.
- Quarantined rows are absent: a row in `order_detail_quarantine` does not appear in `sales_line`.

### Talking points

> "The `sales_line` Materialised View is the integrated enterprise view — one row per order line, all header attributes denormalised. Every pipeline run recomputes it, so a correction in Silver propagates automatically to all downstream consumers."

---

## Segment 7 — Gold KPI aggregates

### Setup

- Segment 1 complete. Gold DLT pipeline has run at least once. `DATAMART` schema contains data.

### Steps

**Step A — Query the truck KPI aggregate:**

1. Run:

   ```sql
   SELECT order_date, truck_id, total_orders, total_revenue, avg_order_value
   FROM   DEMO.DATAMART.daily_sales_by_truck
   ORDER  BY order_date DESC, total_revenue DESC
   LIMIT  20;
   ```

2. Point out any row where `truck_id IS NULL` — this is the intentional "Unknown" bucket for orders where Bronze had no truck assignment.

**Step B — Query the location KPI aggregate:**

3. Run:

   ```sql
   SELECT order_date, location_id, total_orders, total_revenue, avg_order_value
   FROM   DEMO.DATAMART.daily_sales_by_location
   ORDER  BY total_revenue DESC
   LIMIT  20;
   ```

**Step C — Query the monthly currency trend:**

4. Run:

   ```sql
   SELECT year_month, order_currency, total_orders, total_revenue, avg_order_value
   FROM   DEMO.DATAMART.monthly_revenue_by_currency
   ORDER  BY year_month DESC, total_revenue DESC;
   ```

5. Demonstrate a date filter (no string parsing needed — `year_month` is a `DATE`):

   ```sql
   SELECT year_month, order_currency, total_revenue
   FROM   DEMO.DATAMART.monthly_revenue_by_currency
   WHERE  year_month >= '2024-01-01'
   ORDER  BY year_month, order_currency;
   ```

**Step D — Highlight the "Unknown" bucket:**

6. Run:

   ```sql
   SELECT truck_id, SUM(total_revenue) AS unknown_revenue
   FROM   DEMO.DATAMART.daily_sales_by_truck
   WHERE  truck_id IS NULL
   GROUP  BY truck_id;
   ```

   If any rows are returned, these represent orders with no truck attribution — surfaced explicitly rather than silently dropped.

### Expected observation

- `daily_sales_by_truck` and `daily_sales_by_location` each have one row per (date, dimension key) combination.
- `monthly_revenue_by_currency` returns months as `DATE` values (e.g. `2024-03-01`) — BI tools render them on a time axis automatically.
- If a NULL-truck row exists, it appears as a single row in the aggregate rather than being silently discarded.

### Talking points

> "Silver passes `truck_id IS NULL` rows as warnings — they land in the cleansed Silver table and propagate to Gold as a visible 'Unknown' bucket. Data attribution issues surface in the aggregate rather than being silently swallowed."

---

## Segment 8 — Gold wide table + Liquid Clustering

### Setup

- Segment 1 complete. `DATAMART.sales_lines_wide` exists and contains data.

### Steps

**Step A — Inspect the wide table:**

1. Run:

   ```sql
   SELECT order_detail_id, order_id, truck_id, location_id, order_date, order_hour,
          order_day_of_week, order_currency, quantity, unit_price, line_subtotal,
          shift_duration_minutes
   FROM   DEMO.DATAMART.sales_lines_wide
   LIMIT  10;
   ```

2. Point out the derived columns: `order_date`, `order_hour` (0–23), `order_day_of_week` (e.g. "Monday"), `line_subtotal` (quantity × unit_price), `shift_duration_minutes`.

**Step B — Confirm Liquid Clustering:**

3. Run:

   ```sql
   SHOW TBLPROPERTIES DEMO.DATAMART.sales_lines_wide;
   ```

4. Find the row: `clusteringColumns = [["truck_id"],["location_id"],["order_date"],["order_currency"]]`.

5. Demonstrate a clustered query:

   ```sql
   SELECT order_date, truck_id, order_currency,
          COUNT(*) AS line_count, SUM(line_subtotal) AS subtotal
   FROM   DEMO.DATAMART.sales_lines_wide
   WHERE  truck_id = 1
   AND    order_date >= '2024-01-01'
   GROUP  BY order_date, truck_id, order_currency
   ORDER  BY order_date;
   ```

**Step C — Demonstrate time-of-day analysis (Genie-ready):**

6. Run:

   ```sql
   SELECT order_hour, order_day_of_week,
          COUNT(*) AS line_count,
          ROUND(AVG(line_subtotal), 2) AS avg_line_value
   FROM   DEMO.DATAMART.sales_lines_wide
   GROUP  BY order_hour, order_day_of_week
   ORDER  BY order_day_of_week, order_hour;
   ```

### Expected observation

- `SHOW TBLPROPERTIES` shows `clusteringColumns` with the four declared keys.
- The derived time columns (`order_hour`, `order_day_of_week`) are populated correctly from `order_ts`.
- `line_subtotal = quantity * unit_price` — customers can spot `price` vs `line_subtotal` discrepancies.
- The clustered query runs faster on repeat execution as Databricks applies the clustering automatically.

### Talking points

> "Liquid Clustering means no more partition strategy debates. We declare four cluster keys; Databricks automatically reorganises the data as query patterns evolve. No ALTER TABLE, no data migration."

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
   | Revenue trend (line chart) | `DATAMART.monthly_revenue_by_currency` | Monthly revenue over time, one line per currency |
   | Top trucks by revenue (bar chart) | `DATAMART.daily_sales_by_truck` | Top 10 trucks ranked by total revenue |
   | Top locations by revenue (bar chart) | `DATAMART.daily_sales_by_location` | Top 10 locations ranked by total revenue |
   | KPI card | `DATAMART.daily_sales_by_truck` | Total revenue + total orders across all time |

4. Click the **Refresh** button to pull the latest data from the Gold tables.

5. Demonstrate the date filter (if present in the dashboard): adjust the date range and observe that all widgets update simultaneously.

6. Optionally, click **Edit** to show that the dashboard definition is backed by the checked-in file `dashboards/tasty_bytes_sales.lvdash.json` — it is versioned in Git and deployed automatically by the bundle.

### Expected observation

- All four widgets render with data.
- Refreshing the dashboard queries the Gold tables directly — no intermediate cache or import step.
- The dashboard was deployed automatically by `databricks bundle deploy` — no manual creation in the UI was needed.

### Talking points

> "The dashboard is code — it lives in the Git repo, deploys with the bundle, and is always in sync with the pipeline schema. No manual dashboard rebuild after a schema change."

---

## Segment 10 — AI/BI Genie space (post-deploy setup + live demo)

### 10a — First-time setup (run once after bundle deploy)

**Prerequisites:** `dlt_datamart` pipeline has run at least once and `DATAMART.sales_lines_wide` contains data.

1. Navigate to **AI/BI** → **Genie** in the Databricks workspace sidebar.
2. Click **New Genie space** (or the **+** button).
3. Enter the name: **`tasty_bytes_genie`**.
4. Under **Tables**, click **Add table** and select:
   - Catalog: `DEMO`
   - Schema: `DATAMART`
   - Table: `sales_lines_wide`
5. Click **Save**.
6. Under **Example questions**, add the following four questions (copy-paste each):
   - `Welke truck had vorige week de meeste revenue?`
   - `Vergelijk revenue per uur van de dag tussen truck 1 en truck 2`
   - `Wat is de gemiddelde order value per locatie deze maand?`
   - `Welke menu_item_id wordt het meest verkocht op zondag?`
7. (Optional) Configure column-level metadata:
   - Click on the `sales_lines_wide` table in the Genie space editor.
   - Find column `order_hour` and add the description: `The hour of the day the order was placed (0-23).`
   - Find column `order_day_of_week` and add: `The day of the week the order was placed (e.g. Monday, Tuesday).`
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
4. Click **Show SQL** to reveal the generated query. Observe that it filters on `order_date` in the correct date range for "vorige week" and aggregates `order_total` (or `line_subtotal`) per `truck_id`.
5. Ask a follow-up question:

   ```
   Vergelijk revenue per uur van de dag tussen truck 1 en truck 2
   ```

6. Observe that Genie uses the `order_hour` column and generates a side-by-side comparison. This works because `sales_lines_wide` already contains the derived `order_hour` column.

### Expected observation

- Genie answers the question in natural language and shows a SQL query.
- The generated SQL references `DEMO.DATAMART.sales_lines_wide` (the only table in the space).
- The `order_hour` and `order_day_of_week` derived columns make time-of-day and day-of-week questions answerable without any additional join or calculation.

### Talking points

> "Genie answers business questions in natural language using the data we built in the Gold layer. The derived columns — hour of day, day of week — make these questions answerable without the analyst needing to know the underlying schema."

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

> "Delta Lake's transaction log is a built-in time machine. No back-up or restore procedure required — every version of the control table is queryable with a single SQL clause. This is how compliance teams reconstruct data state for a specific reporting date."

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

> "The Unity Catalog audit log records every data access and modification — automatically, without any instrumentation. Combined with Delta Time Travel, we have both the 'who' (audit log) and the 'what' (Delta version history) for every change to every table."

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

**Section 2 — Bronze lineage:**

Visual step-by-step in Catalog Explorer:

1. Navigate to **Catalog** → `DEMO` → `STAGING_AZURESTORAGE` → `order_header` → **Lineage** tab.
2. The upstream node is the parquet volume path `/Volumes/demo/staging_azurestorage/parquet`.

Programmatic check:

```sql
SELECT source_table_full_name, target_table_full_name, entity_type
FROM   system.lineage.table_lineage
WHERE  LOWER(target_table_full_name) = 'demo.staging_azurestorage.order_header'
ORDER  BY created_at DESC;
```

**Section 3 — Silver lineage:**

Visual:

1. Navigate to `DEMO` → `INTEGRATION` → `sales_line` → **Lineage** tab.
2. Two upstream nodes: `INTEGRATION.order_header` and `INTEGRATION.order_detail`.
3. Click the **Column lineage** tab. Select column `order_total`. Trace it back to `order_header.order_total` in Bronze.

Programmatic check:

```sql
SELECT source_table_full_name, target_table_full_name
FROM   system.lineage.table_lineage
WHERE  LOWER(target_table_full_name) = 'demo.integration.sales_line';
```

**Section 4 — Gold lineage (aggregates):**

Navigate to `DEMO` → `DATAMART` → `daily_sales_by_truck` → **Lineage** tab. One upstream node: `INTEGRATION.order_header`.

**Section 5 — Gold lineage (wide table, full chain):**

Navigate to `DEMO` → `DATAMART` → `sales_lines_wide` → **Lineage** tab. Trace the full chain:

```
parquet files → order_header (Bronze) → order_header (Silver)
                                                         ↘
                                                          sales_line (Silver MV)
parquet files → order_detail (Bronze) → order_detail (Silver)       ↓
                                                          sales_lines_wide (Gold)
```

**Section 6 — Programmatic lineage + impact analysis:**

The notebook runs a recursive CTE to show the full downstream chain from `STAGING_AZURESTORAGE.order_header`:

```sql
-- Which tables are affected if I change STAGING_AZURESTORAGE.order_header?
SELECT DISTINCT target_table_full_name
FROM   system.lineage.table_lineage
WHERE  LOWER(source_table_full_name) = 'demo.staging_azurestorage.order_header';
```

**Section 7 — Governance narrative:**

Summarises the four lineage use cases: impact analysis, debugging, compliance, and automatic catalogisation.

### Expected observation

- The Catalog Explorer Lineage tab shows the medallion chain visually: parquet → Bronze → Silver → Gold.
- Column-level lineage traces `order_total` from Gold back to the Bronze source column.
- The recursive CTE query returns all downstream tables for a given Bronze table.
- All lineage was captured automatically — no manual registration, API calls, or annotations were made.

### Talking points

> "Unity Catalog captures lineage automatically — from raw parquet files all the way through to Gold KPI aggregates and dashboards. Column-level lineage means you can trace any metric in a dashboard back to its exact source column in the raw data. No separate data catalog product required."

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
-- See recent change events on a Bronze table
SELECT _change_type, _commit_version, order_id, order_total
FROM   table_changes('DEMO.STAGING_AZURESTORAGE.order_header', 1)
ORDER  BY _commit_version DESC
LIMIT  20;

-- Confirm CDF is enabled
SHOW TBLPROPERTIES DEMO.STAGING_AZURESTORAGE.order_header;
```

### Quarantine triage

```sql
-- All quarantine rows for order_header
SELECT order_id, order_total, failed_rules
FROM   DEMO.INTEGRATION.order_header_quarantine
ORDER  BY order_id DESC
LIMIT  20;

-- Filter by specific failed rule
SELECT order_id, order_total, failed_rules
FROM   DEMO.INTEGRATION.order_header_quarantine
WHERE  array_contains(failed_rules, 'order_total_non_negative');

-- Detail quarantine
SELECT order_detail_id, quantity, failed_rules
FROM   DEMO.INTEGRATION.order_detail_quarantine
WHERE  array_contains(failed_rules, 'quantity_positive');
```

### Silver integration view

```sql
SELECT order_detail_id, order_id, menu_item_id, quantity, order_total, truck_id, order_ts
FROM   DEMO.INTEGRATION.sales_line
LIMIT  10;
```

### Gold KPIs

```sql
-- Top trucks by revenue
SELECT truck_id, SUM(total_revenue) AS revenue
FROM   DEMO.DATAMART.daily_sales_by_truck
GROUP  BY truck_id
ORDER  BY revenue DESC
LIMIT  10;

-- Monthly trend
SELECT year_month, order_currency, total_revenue
FROM   DEMO.DATAMART.monthly_revenue_by_currency
ORDER  BY year_month DESC, total_revenue DESC;

-- Liquid Clustering keys
SHOW TBLPROPERTIES DEMO.DATAMART.sales_lines_wide;
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
-- Downstream impact from Bronze order_header
SELECT DISTINCT target_table_full_name
FROM   system.lineage.table_lineage
WHERE  LOWER(source_table_full_name) = 'demo.staging_azurestorage.order_header';

-- Upstream trace for Gold wide table
SELECT source_table_full_name, target_table_full_name
FROM   system.lineage.table_lineage
WHERE  LOWER(target_table_full_name) = 'demo.datamart.sales_lines_wide';
```

---

## Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| `ingest_azurestorage_incremental` task skips all tables | All rows in control table have `load_type='full'` | Segment 3: run the `UPDATE` to switch at least one row to `incremental` |
| Silver pipeline halts with "FAIL expectation violated" | A row with `order_id IS NULL` or `order_detail_id IS NULL` is in Bronze | Delete the bad row from Bronze and re-trigger the pipeline |
| `system.access.audit` not found | System Tables not enabled | Account Console → Metastore → System schemas → enable `access` |
| `system.lineage.table_lineage` not found | Lineage System Tables not enabled | Account Console → Metastore → System schemas → enable `lineage` |
| Genie space not showing `sales_lines_wide` | Gold pipeline has not run | Run the full workflow first; wait for `dlt_datamart` to complete |
| Dashboard shows empty widgets | Gold tables empty or warehouse not running | Start the SQL warehouse; run the full workflow |
| `table_changes()` returns empty | CDF was not enabled at table creation time | The setup notebook creates Bronze tables with CDF enabled; if tables were created manually, run `ALTER TABLE ... SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')` |
