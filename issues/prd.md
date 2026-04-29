# PRD: Databricks demo template — Bronze layer with Lakeflow Connect, DLT, and metadata-driven ingestion

## Problem Statement

We need a Databricks demo environment that showcases platform best practices to prospective customers. The demo must feel production-ready in setup and explanation, even though only a single environment is fully wired. Customers should be able to walk through it and reproduce the pattern themselves.

The demo must concretely demonstrate:

- **Unity Catalog** governance and environment isolation (DEV / TEST / PROD as separate catalogs)
- **Lakeflow Connect** for ingestion from operational databases (Azure SQL Server with CDC)
- **Auto Loader** for incremental ingestion from cloud storage (Azure Storage parquet)
- **Delta Live Tables (DLT)** with Expectations for declarative data quality
- **Databricks Workflows** for orchestration
- **Databricks Asset Bundles (DAB)** for IaC deployment
- **Delta Time Travel**, **UC Audit Logs**, and **UC Lineage** as platform features
- A **metadata-driven ingestion pattern** where a single SQL UPDATE flips a pipeline's behaviour from full to incremental

The existing repo has a partial Lakeflow Connect scaffold for SQL Server but no `databricks.yml`, no parquet-side ingestion, no DLT pipeline, no orchestrating Workflow, and no bootstrap for the Unity Catalog structure. The architecture is captured at a high level in `databricks_demo_architecture.md` but contains several gaps and one direct conflict (custom audit columns vs Lakeflow Connect's managed tables).

## Solution

A Databricks Asset Bundle that, once deployed against a workspace with prerequisites in place, provisions a complete Bronze-layer demo:

1. Creates the `DEMO_DEV` catalog, three schemas (`STAGING_AZURESTORAGE`, `STAGING_SQLSERVER`, `CONFIG`), an external volume, and a control table seeded with two parquet ingestion rows.
2. Runs a control-table-driven Auto Loader notebook that ingests `ORDER_HEADER_*.parquet` and `ORDER_DETAIL_*.parquet` into Delta tables, with five custom audit columns. The notebook supports both full and incremental load modes via the `load_type` column in the control table.
3. Runs a parallel DLT pipeline that ingests the same parquet source into separate `_dlt`-suffixed tables, demonstrating Expectations (three severity levels) and the DLT graph view.
4. Runs the existing Lakeflow Connect ingestion for SQL Server, retargeted to `DEMO_DEV.STAGING_SQLSERVER`, on a daily 04:00 UTC schedule via a separate Job that shares the pipeline resource.
5. Provides three demo-showcase notebooks for Time Travel, Audit Logs, and Lineage walkthroughs.
6. Provides a manual demo script document with step-by-step talking points and SQL snippets for live customer demos.

`DEMO_TEST` and `DEMO_PROD` catalogs exist as named, empty shells in the bundle's target definitions to demonstrate the layout pattern without provisioning real pipelines for them.

## User Stories

1. As a Databricks pre-sales engineer, I want a deployable demo bundle, so that I can stand up a fresh customer-facing demo in one workspace command.
2. As a pre-sales engineer, I want the demo to use Lakeflow Connect for SQL Server ingestion, so that I can showcase Databricks' flagship CDC ingestion capability for operational databases.
3. As a pre-sales engineer, I want the demo to use Auto Loader for parquet ingestion, so that I can showcase Databricks' recommended pattern for incremental file ingestion.
4. As a pre-sales engineer, I want a metadata-driven control table, so that I can demonstrate live to a customer that one SQL UPDATE flips a pipeline from full to incremental load with no code change.
5. As a pre-sales engineer, I want a parallel DLT pipeline writing to separate tables, so that I can compare hand-rolled and declarative ingestion side by side in the same workspace.
6. As a pre-sales engineer, I want the DLT pipeline to use Expectations of escalating severity, so that I can demonstrate data-quality enforcement by intentionally dropping a bad parquet file mid-demo.
7. As a pre-sales engineer, I want orchestration via a Databricks Workflow, so that I can show the end-to-end pipeline graph as a single click-to-run unit in the UI.
8. As a pre-sales engineer, I want the SQL Server CDC refresh on its own schedule, so that I can control when refreshes fire and demo CDC behaviour on demand from the Pipelines UI.
9. As a pre-sales engineer, I want three Unity Catalog catalogs visible (DEV/TEST/PROD), so that I can show customers the recommended environment-isolation pattern even though only DEV is wired.
10. As a pre-sales engineer, I want every staging table to carry source-appropriate audit columns, so that I can explain provenance without papering over the difference between file ingest and database CDC.
11. As a pre-sales engineer, I want the parquet staging tables to carry five custom audit columns, so that I can show how to enrich file-based ingestion with metadata.
12. As a pre-sales engineer, I want the SQL Server staging tables to expose Lakeflow Connect's built-in CDC columns, so that I can show real change-tracking metadata rather than synthetic columns.
13. As a pre-sales engineer, I want a Time Travel demo notebook, so that I can show customers how to query a previous version of the control table.
14. As a pre-sales engineer, I want a UC Audit Logs demo notebook, so that I can show customers who-did-what governance over the demo objects.
15. As a pre-sales engineer, I want a UC Lineage demo notebook, so that I can walk customers through which pipeline produced which table in the Lineage viewer.
16. As a pre-sales engineer, I want a manual demo-script document, so that I can rehearse and run live customer demos with concrete talking points and SQL snippets ready to paste.
17. As a pre-sales engineer, I want a `prerequisites.md` document, so that customers attempting to reproduce the demo know exactly which Azure and Unity Catalog admin steps must happen first.
18. As a pre-sales engineer, I want the bundle to deploy from Azure DevOps, so that the demo matches the source-control story I'm telling enterprise customers.
19. As a Databricks workspace admin (the customer reproducing the demo), I want all DAB resources externalised as variables, so that I can retarget the bundle at my workspace, my catalog, and my UC connections without editing source files.
20. As a workspace admin, I want a single bootstrap notebook that idempotently creates the catalog, schemas, volume, and control table, so that re-running it after a partial failure converges on the desired state.
21. As a workspace admin, I want the Workflow defined in YAML alongside the notebooks, so that the orchestration is auditable and version-controlled.
22. As a workspace admin, I want the SQL Server pipeline schedule decoupled from the demo Workflow, so that scheduled refreshes don't fire during a customer call.
23. As a customer watching the demo, I want to see the same parquet data flow through two different ingestion patterns, so that I understand what DLT specifically buys me over a hand-rolled notebook.
24. As a customer, I want to see Lineage in the UC viewer, so that I trust I can answer downstream "where did this column come from?" questions in production.
25. As a customer, I want to see how Databricks handles CDC end-to-end, so that I can compare it against my current ETL or replication tooling.

## Implementation Decisions

### Source-of-truth resolutions of architecture-doc gaps

- **SQL Server ingestion uses Lakeflow Connect**, not a custom notebook. The existing `resources/sqlserver.yml` is the right pattern; only its hardcoded values need replacing with bundle variables.
- **`DEMO_TEST` and `DEMO_PROD` are visual-only**. Only `DEMO_DEV` is fully wired with pipelines and data. The `targets:` block in `databricks.yml` defines all three for completeness.
- **"Incremental" means Auto Loader** for the parquet source. The control table needs no watermark or merge-key columns; checkpoints handle state. The basic ingest notebook branches on `load_type` between a full overwrite read and a `cloudFiles` stream.
- **Audit columns split by source.** Parquet ingestion (basic notebook + DLT) writes five custom columns: `_ingestion_timestamp`, `_source_system`, `_source_file`, `_last_modified`, `_pipeline_run_id`. Lakeflow Connect target tables surface their own CDC columns: `_change_type`, `_change_version`, `_commit_timestamp`. This conflict in the architecture doc is resolved in favour of source-appropriate columns.
- **Orchestration is a Databricks Workflow**, not a master notebook. `dbutils.notebook.run()` cannot trigger DLT or Lakeflow Connect pipelines. Section 11 of the architecture doc is overridden.
- **The SQL Server Job and the demo Workflow share one pipeline resource.** The Lakeflow Connect pipeline is a first-class DAB resource; both the scheduled Job (`sqlserver_job.yml`) and the end-to-end Workflow (`demo_workflow.yml`) reference it via `pipeline_id`. The pipeline can also be triggered manually from the UI mid-demo.
- **The DLT pipeline does not read the control table.** It is parameterised on catalog only. The metadata-driven-switch demo is exclusively a basic-pipeline feature; the DLT pipeline carries the data-quality / declarative-graph story instead.
- **The parquet source layout is flat**, not foldered. All `ORDER_HEADER_*.parquet` and `ORDER_DETAIL_*.parquet` files sit directly in the container's `source/` folder. Routing to per-table targets is done via Auto Loader's `pathGlobFilter`, configured per row in the control table via a new `file_pattern` column.
- **Bootstrap is one notebook**: `00_bootstrap.py` creates the catalog, three schemas, the external volume, and the control table with two seed rows. It is idempotent (`CREATE … IF NOT EXISTS` and `MERGE`-style seed inserts).
- **Three layers of bootstrap exist**: Layer 1 cloud-side admin setup (Access Connector, Storage Credential, External Location, UC Connection — manual, documented in `prerequisites.md`), Layer 2 catalog/schema/volume/control-table bootstrap (one notebook in the bundle), Layer 3 source-data seeding (parquet files already exist in Azure Storage; SQL Server table exists with name TBD).

### Module inventory

**Notebooks (Python):**

- Bootstrap notebook — creates catalog, schemas, external volume, control table with two seed rows. Idempotent.
- Basic Auto Loader ingest notebook — reads control table for active `azurestorage` rows, branches on `load_type`, applies `pathGlobFilter` per row, adds five custom audit columns, writes per-table Delta targets.
- DLT staging pipeline source — two `@dlt.table` definitions (`order_header_dlt`, `order_detail_dlt`), each with three Expectations of escalating severity (`expect`, `expect_or_drop`, `expect_or_fail`), Auto Loader inside, custom audit columns.
- Time Travel showcase notebook — SQL queries against control-table versions including `VERSION AS OF` and `TIMESTAMP AS OF`.
- Audit Logs showcase notebook — queries against UC system audit logs filtered to demo objects.
- Lineage showcase notebook — narrative walkthrough referencing the UC Lineage viewer.

**DAB resources (YAML):**

- Bundle root (`databricks.yml`) — bundle name, `include: resources/*.yml`, variables (`catalog`, `notification_email`, `sqlserver_connection_name`, `azure_storage_external_location`), three targets (`dev` with real values, `test`/`prod` as stubs).
- SQL Server resource — Lakeflow Connect gateway + ingestion pipeline; catalog/schema/connection-name parameterised via variables.
- SQL Server Job — daily 04:00 UTC trigger of the SQL Server pipeline only; notification email parameterised.
- DLT staging pipeline resource — pipeline definition referencing the DLT notebook, target schema, channel.
- Demo Workflow — end-to-end Workflow with notebook tasks (bootstrap → parquet ingest) and pipeline tasks (DLT pipeline, SQL Server pipeline) where parallelisable.

**Documentation:**

- Prerequisites doc — Layer 1 manual admin setup with concrete UI/CLI steps.
- Demo script doc — manual walkthrough for live customer demos with talking points and SQL snippets.
- Architecture doc updates — the seven section edits to `databricks_demo_architecture.md` have already been applied in this session; remaining work is to keep the doc in sync as implementation proceeds.

### Schema additions

Control table gets a new `file_pattern` column of type `string`. Seed rows updated to populate it for each parquet target. No watermark or merge-key columns are added; Auto Loader checkpoints handle incremental state.

### Bundle variable contract

The bundle exposes four variables that targets must populate: `catalog` (the UC catalog), `notification_email` (job-failure recipient), `sqlserver_connection_name` (UC Connection wrapping SQL Server credentials), `azure_storage_external_location` (UC External Location wrapping the Azure Storage container). All four are populated for the `dev` target; the `test` and `prod` targets carry empty placeholders.

## Testing Decisions

No automated tests are written. The realistic alternatives — pytest against Spark notebooks, integration test harnesses for DLT — give little value for a demo whose audience is humans watching a UI.

The end-to-end Workflow run is the integration test: if it goes green, the demo is healthy. The DLT pipeline's Expectations metrics are a continuous data-quality check visible in the UI.

In place of automated tests, a `docs/demo_script.md` document captures **manual demo steps** with concrete commands and expected observations. These double as smoke-tests the pre-sales engineer runs before customer calls. The document covers, at minimum:

- Verifying the bootstrap notebook is idempotent (run twice, observe stable row counts).
- The full → incremental switch demo (live SQL UPDATE, re-run notebook, observe behaviour change).
- The DLT Expectations demo (drop a deliberately malformed parquet file into the volume, re-run, show dropped-row counters in the DLT UI).
- The Lakeflow Connect CDC demo (insert/update/delete rows in the SQL Server source, manually trigger the pipeline, observe the `_change_type` column populate).
- The Time Travel demo (SQL queries showing `VERSION AS OF` and `TIMESTAMP AS OF`).
- The Audit Logs demo (UC audit-log queries showing the demo's own activity).
- The Lineage demo (Lineage viewer walkthrough screenshots and talking points).

## Out of Scope

- The Integration (Silver) and Datamart (Gold) layers — section 1 of the architecture doc reserves these schemas but their content is explicitly deferred.
- Real provisioning of `DEMO_TEST` and `DEMO_PROD`. Their `targets:` blocks in `databricks.yml` carry empty placeholders for variables; no pipelines run there.
- The Azure DevOps repo migration mechanics. The code lives in Azure DevOps eventually but the bundle and PRD do not specify the migration steps from the current GitHub repo.
- Service-principal identity for the `prod` target. `mode: production` requires a service principal in real use; the bundle declares `mode: production` for `prod` but does not provision the SP.
- The SQL Server source table name. Recorded as a known-unknown to be filled in once available; the bundle's variable contract accommodates it without code changes.
- Automated tests of any kind.
- Continuous-mode Lakeflow Connect pipelines. Mentioned narratively but not the demo default.
- A managed-tables version of the parquet target (the demo writes to managed Delta tables in UC by default; Iceberg / external Delta is not covered).

## Further Notes

- The current GitHub remote (`AOEshun/databricks-demo`) hosts the public version of this PRD as issue #1 and the supporting issue tracker. The Databricks Repos integration in the customer-facing workspace will point at Azure DevOps; the move is operational, not architectural.
- The Lakeflow Connect connection name `rebel` in the existing `resources/sqlserver.yml` is a placeholder and is replaced by the `sqlserver_connection_name` bundle variable.
- The current `sqlserver_job.yml` has a 6-hour schedule and a `<user-email>` placeholder. Both are corrected: schedule becomes daily at 04:00 UTC, email becomes the `notification_email` variable.
- Three Expectation severity levels in the DLT pipeline: `expect` (warn, row passes), `expect_or_drop` (quarantine, pipeline continues), `expect_or_fail` (halt). All three are exercised so the customer sees each behaviour.
- Auto Loader checkpoints live under `_checkpoints/{target_table}/` inside the external volume. Re-running a notebook in incremental mode resumes from the last checkpoint, satisfying the architecture doc's idempotency requirement.
- The DLT pipeline's run id is exposed via `spark.conf.get("pipelines.id")` and used for the `_pipeline_run_id` audit column. The basic notebook uses the Workflow-populated job run id widget.
