# PRD: KRM/ADR-conformance refactor of the integration and datamart layers

## Problem Statement

The codebase implements the original "Silver/Gold snake_case medallion" plan from the previous PRD, but a subsequent KRM-alignment session pinned a different target via ADRs 0003–0020 (plus `naamgeving-en-lagen.md`). The two are now openly inconsistent:

- **Naming.** ADRs and naamgeving mandate uppercase prefixed identifiers (`STG_`, `DW_`, `DWH_`, `DWQ_`, `DIM_`, `FCT_`). Code uses lowercase snake_case (`order_header`, `order_header_quarantine`, `dim_truck`, `fact_order`, `fact_sales_line`).
- **Historisation.** ADR-0010 mandates SCD Type 2 via `APPLY CHANGES INTO ... STORED AS SCD TYPE 2` reading from `STREAM table_changes()` over the staging table's Change Data Feed. Code uses SCD Type 1 via `APPLY CHANGES INTO ... FROM SNAPSHOT`, so version history is not retained.
- **DW + DWH twin pattern.** ADR-0010, ADR-0011 and ADR-0020 require a `DW_<TABEL>` storage table with an `IDENTITY` `WK_<TABEL>` surrogate and `__START_AT` / `__END_AT` managed by `FLOW AUTO CDC`, paired with a `DWH_<TABEL>` view that renames the validity columns to `WA_FROMDATE` / `WA_UNTODATE` / `WA_ISCURR` and computes `WKP_<TABEL>` / `WKR_<TABEL>` via window functions. Code has neither the DW IDENTITY surrogate nor any DWH view.
- **Quarantine pattern.** ADR-0011 mandates a single tagged source materialised view that computes `failed_rules ARRAY<STRING>` once, feeding both the cleansed `DW_<TABEL>` (`WHERE size(failed_rules) = 0`) and the paired `DWQ_<TABEL>` (`WHERE size(failed_rules) > 0`), with `CONSTRAINT ... EXPECT (NOT array_contains(failed_rules, '<rule>'))` declarations so each rule's violation count surfaces in the DLT event log. Code maintains two parallel typing-and-rule MVs (`_clean_src` + `_quarantine_src`), duplicating the rule logic across files and missing the per-rule expectation declarations.
- **Admin columns.** Staging must carry `SA_CRUDDTS` / `SA_SRC` / `SA_RUNID` (ADR-0017 for Auto Loader sources; Lakeflow Connect sources legitimately omit them). DW must carry `WA_CRUDDTS` / `WA_CRUD` / `WA_SRC` / `WA_RUNID` / `WA_HASH` (ADR-0015, ADR-0017). Datamart SCD1 dims must carry `MK_<TABEL>` / `MA_CREATEDATE` / `MA_CHANGEDATE` / `MA_ISDEL`; SCD2 dims must carry `MK_<TABEL>` / `MK_ROOT` / `MA_FROM` / `MA_UNTO` / `MA_ISCURR`. Code carries the previous `_ingestion_timestamp` / `_source_system` / `_source_file` / `_last_modified` / `_pipeline_run_id` family in staging, propagates them unchanged through integration, and exposes no admin columns on dims at all.
- **Surrogates.** ADR-0010 / ADR-0012 require `WK_<TABEL>` BIGINT IDENTITY (lineage version-level), `WKR_<TABEL>` BIGINT (lineage root, used as `MK_<TABEL>` on SCD1 dims and as `MK_ROOT` on SCD2 dims). ADR-0014 keeps a `WK_REF_HASH_<REF>` FK-side hash but rejects the self-side `WKR_HASH_<TABEL>`. Code instead computes a per-dim `SHA2(<natural_key>, 256)` inline on both dim side and fact side — the consumer-keyed surrogate model the original Silver/Gold PRD set out, not the IDENTITY-lineage model the KRM ADRs require.
- **DIM_DATE.** ADR-0018 fixes `MK_DATE` as the date encoded as a `yyyymmdd` integer. Code uses `SHA2(CAST(full_date AS STRING), 256)`.
- **Fact source.** ADR-0020 requires `FCT_*` MVs to read from `DWH_<TABEL>` (with the half-open SCD2 temporal interval where applicable), projecting `WKR_<TABEL>` as the `MK_<TABEL>` FK. Code reads `fact_order` from `INTEGRATION.order_header` and `fact_sales_line` from `INTEGRATION.sales_line` (a header⨝detail join view), computing `SHA2` FKs inline.
- **Integration layer canonicalisation.** ADR-0016 requires one canonical `DW_<TABEL>` per business entity (`WA_SRC` distinguishes provenance), not a per-source variant — and this is the policy that lets the parked SQL Server source feed `DW_<TABEL>` alongside the Azure Storage source without renaming.
- **Topology and orchestration.** ADR-0020 lays out the task graph as `setup → ingest_* → dlt_integration → (dlt_datamart ‖ apply_views)`. The current workflow runs `apply_views` before `dlt_datamart` because `fact_sales_line` reads the `sales_line` view; under ADR-0020 facts read `DWH_` directly, removing that dependency. Pipeline names also still use Silver/Gold vocabulary (`demo-silver-integration`, `demo-gold-datamart`).
- **Documentation drift.** `CONTEXT.md` §6–§9 still describes the current snake-case Silver/Gold implementation; ADRs 0001 and 0002 still use Silver/Bronze vocabulary that ADRs 0003–0020 have moved past; `naamgeving-en-lagen.md` has several formulas (`MK_ ← WK_`, `MA_CHANGEDATE` filter ignoring deletes, "alleen `WA_ISCURR = 1`" SCD1 filter) that ADR-0012 explicitly corrects.

`issues/krm-adr-followups.md` already enumerates this work as Follow-ups A (docs), B (ADR housekeeping) and C (code refactor); this PRD is the formal capture of the same scope so it can be broken into `/to-issues` tickets.

## Solution

Bring the codebase, the DAB resources, the dashboard, and the project documentation into conformance with the binding ADRs (0003–0020). The KRM staging / integration / datamart layer vocabulary, the uppercase prefixed identifier convention, the SCD2 DW + paired DWH view pattern, the tagged-MV-with-`failed_rules` quarantine pattern, the SCD1/SCD2 dim specs with `MK_` / `MA_` admin columns, the `DWH_`-direct fact build with `WKR_`-as-`MK_` surrogate resolution, and the ADR-0020 task graph all replace the equivalent constructs in current code.

The refactor is non-incremental in the user-visible sense — table names change, schemas re-shape, dashboard queries rewire — and is delivered as a coordinated multi-PR effort so the deployable artefact stays consistent at every commit boundary. After the refactor, ADRs and code agree, and `CONTEXT.md` becomes a faithful tabelspec inventory pointing at the ADR corpus as the source of truth for decisions.

## User Stories

1. As a demo presenter, I want every table and view name in the live catalog to match the names quoted in the ADRs (`STG_ORDER_HEADER`, `DW_ORDER_HEADER`, `DWH_ORDER_HEADER`, `DWQ_ORDER_HEADER`, `DIM_TRUCK`, `FCT_ORDER`, `FCT_SALES_LINE`, …), so that I can read ADRs aloud during the demo without ad-hoc translation.
2. As a demo presenter, I want to open `DW_ORDER_HEADER` in the catalog and see multiple versions per business key with `__START_AT` / `__END_AT` populated, so that the SCD2 story lands when I narrate "every change is a new row, not an overwrite".
3. As a demo presenter, I want `DWH_ORDER_HEADER` to expose `WA_FROMDATE`, `WA_UNTODATE`, `WA_ISCURR`, `WKP_ORDER_HEADER` and `WKR_ORDER_HEADER`, so that I can show the projection layer that downstream dims and facts join against.
4. As a demo presenter, I want to query `DWQ_ORDER_HEADER` and filter `array_contains(failed_rules, 'order_total_non_negative')` to surface the rows quarantined for that specific rule, so that data-quality routing is a tangible demo moment.
5. As a demo presenter, I want each drop-rule's violation count to appear in the DLT event-log dashboard, so that "we don't silently drop rows" is visible without a custom query.
6. As a demo presenter, I want the DLT pipeline names to read `demo-integration` and `demo-datamart` (KRM layer vocabulary), so that the UI narrative matches the ADRs.
7. As a demo presenter, I want a brand-new business key value to appear in `DW_ORDER_HEADER` as a single row whose `WK_ORDER_HEADER` equals its `WKR_ORDER_HEADER`, so that lineage roots are visible from day one.
8. As a demo presenter, I want a delete in the source to set `__END_AT` on the prior current row in `DW_ORDER_HEADER` (no tombstone row), so that the trade-off ADR-0010 documents is observable.
9. As a demo presenter, I want `DIM_DATE.MK_DATE` to read as `20240115` (integer yyyymmdd) for a row dated 2024-01-15, so that the surrogate is human-recognisable at a glance — per ADR-0018.
10. As a demo presenter, I want `DIM_TRUCK`, `DIM_LOCATION`, `DIM_DISCOUNT` and `DIM_SHIFT` (the four warn-rule dims) to retain rows for business keys that have been end-dated upstream, so that the SCD1 "deleted entity stays visible" promise from ADR-0012 holds.
11. As a demo presenter, I want `MK_TRUCK` on `DIM_TRUCK` to remain stable across updates of the same truck, so that fact joins don't drift as the truck record evolves — `MK_` is the root surrogate, not the version surrogate.
12. As a demo presenter, I want `DIM_<NAAM>` for SCD2 entities to show every version of the entity with `MA_FROM` / `MA_UNTO` / `MA_ISCURR` populated and `MK_ROOT` grouping versions, so that the SCD2 model is queryable in the consumer surface.
13. As a demo presenter, I want `FCT_ORDER` and `FCT_SALES_LINE` to project `WKR_<TABEL>` as their `MK_<TABEL>` FK column (per ADR-0014 / ADR-0020), so that surrogate resolution lives at fact-build time and dim joins are 1-to-1 even across version churn.
14. As a demo presenter, I want fact rows to land against the entity version current at the order's `order_ts` (half-open SCD2 interval on `DWH_<TABEL>`), so that historical fact rows reflect the entity state at event time, not "as-of-now".
15. As a data analyst, I want the staging tables (`STG_ORDER_HEADER`, `STG_ORDER_DETAIL`) to carry `SA_CRUDDTS`, `SA_SRC` and `SA_RUNID`, so that the lakehouse-side ingestion provenance is visible in the canonical admin-column shape.
16. As a data analyst, I want `DW_<TABEL>` rows to carry `WA_CRUDDTS`, `WA_CRUD`, `WA_SRC`, `WA_RUNID` and `WA_HASH`, so that historical lineage, CDC-action provenance and row-content signature are all queryable per row.
17. As a data analyst, I want every `DWQ_<TABEL>` row to carry the same `WA_*` columns as its `DW_<TABEL>` sibling plus `failed_rules`, so that quarantine triage has full audit context, not a stripped-down envelope.
18. As a data analyst, I want `WA_HASH` to use `SHA2(..., 256)` per ADR-0019, so that the algorithm is uniform across the corpus and source-system reconciliation queries compute the same digest both sides.
19. As a data analyst, I want every SCD1 `DIM_<NAAM>` view to carry `MA_CREATEDATE`, `MA_CHANGEDATE` and `MA_ISDEL`, so that "when was this entity first seen / last touched / is it deleted?" are answerable without joining back to DWH.
20. As a data analyst, I want `MA_CHANGEDATE` to include deletes (`WA_CRUD <> 'C'`), so that an end-dated entity shows its delete moment as its last change, not its previous update — per the ADR-0012 correction to naamgeving §2.6.
21. As a data analyst, I want every SCD2 `DIM_<NAAM>` view to carry `MA_FROM`, `MA_UNTO`, `MA_ISCURR`, and `MK_ROOT`, so that temporal slicing and version grouping work directly off the consumer surface.
22. As a data engineer, I want the integration DLT pipeline to read staging via `STREAM table_changes()` (the CDF read pattern of ADR-0010), so that updates and deletes flow into `DW_<TABEL>` as new versions and end-dates instead of disappearing under a snapshot-diff.
23. As a data engineer, I want every per-entity DLT SQL file to use a single tagged source MV (ADR-0011 pattern) that computes the `failed_rules` array exactly once, so that rule logic lives in one place per entity and DW/DWQ stay structurally symmetric.
24. As a data engineer, I want each tagged source MV to carry `CONSTRAINT ... EXPECT (NOT array_contains(failed_rules, '<rule>'))` declarations for every drop-rule, so that the DLT event log surfaces per-rule violation counts per run.
25. As a data engineer, I want `fail`-severity rules to surface as `EXPECT (...) ON VIOLATION FAIL UPDATE` on the `DW_<TABEL>` write so the pipeline halts on schema-level invariants (e.g. `order_id IS NOT NULL`), while `warn`-severity rules surface as `EXPECT (...)` (no `ON VIOLATION` clause) so the row stays in cleansed and the violation count appears in the event log.
26. As a data engineer, I want the Auto Loader-driven ingest notebook to emit `SA_*` admin columns when writing `STG_<TABEL>`, replacing the previous `_ingestion_timestamp` / `_source_system` / `_source_file` / `_last_modified` / `_pipeline_run_id` set, so that staging conforms to ADR-0017 for non-Lakeflow-Connect sources.
27. As a data engineer, I want the parked SQL Server / Lakeflow Connect path to remain ADR-0017-compliant (no synthetic `SA_*` view-wrapper, native `_change_type` / `_change_version` / `_commit_timestamp` columns flow straight into the per-entity integration SQL), so that when the source is unparked it slots in without further refactor.
28. As a data engineer, I want each integration entity to be a canonical `DW_<TABEL>` regardless of source count (`WA_SRC` distinguishing per row, per ADR-0016), so that when SQL Server unparks the same entity it merges into the existing target rather than spawning a `DW_<TABEL>_<SRC>` variant.
29. As a data engineer, I want `FCT_*` materialised views to read `DWH_<TABEL>` directly and not depend on `DIM_<NAAM>` view existence, so that `dlt_datamart` and `apply_views` can run in parallel after `dlt_integration` (the ADR-0020 topology).
30. As a data engineer, I want the AI/BI Dashboard JSON to be regenerated against the renamed tables and columns (`FCT_ORDER ⨝ DIM_TRUCK`, etc.) so that `databricks bundle deploy` continues to ship a working dashboard after the refactor.
31. As a data engineer, I want every DAB resource file (workflow, two DLT pipelines, dashboard, the parked Lakeflow Connect pair) to reference the renamed schemas, tables, pipeline names and task graph, so that a clean deploy produces the ADR-target catalog from zero.
32. As a data engineer, I want the `apply_views` orchestrator and its child view notebooks (`integration/sales_line`, `datamart/dim_*`) to be updated for the renamed source tables (`DWH_<TABEL>`) and the new dim specs (SCD1 latest-row-per-BK, SCD2 expose-every-version, `MK_` / `MA_*` admin columns), so that the consumer surface matches ADR-0012 / ADR-0013 / ADR-0018.
33. As a data engineer, I want `INTEGRATION.SALES_LINE` (a header⨝detail business view) to remain a plain UC view but rebuilt against `DWH_<TABEL>` rather than the cleansed `DW_<TABEL>`, so that the view picks the entity version current at the line event time.
34. As a data engineer, I want the dashboard's `lvdash.json` widget queries to reference `FCT_ORDER` / `FCT_SALES_LINE` and the renamed `DIM_*` joins, so that nothing in the consumption layer references stale names after deploy.
35. As a documentation reader, I want `CONTEXT.md` §6, §7, §8 and §9 rewritten against the ADR target state, so that the project guide and the binding decisions agree on naming, mechanism, and topology.
36. As a documentation reader, I want `CONTEXT.md` to standardise its tabelspec sections to a single shape per entity and apply it across integration and datamart, so that scanning the doc for a given entity is consistent.
37. As a documentation reader, I want every Silver / Bronze / Gold mention purged from `CONTEXT.md` in favour of staging / integration / datamart (or KRM's Ingest / Combine / Publish where the abstract layer name is needed), so that vocabulary drift is closed.
38. As a documentation reader, I want ADR-0001 and ADR-0002 reconciled with the post-KRM ADR corpus — either superseded by ADR-0007 + ADR-0011 (for 0001) and ADR-0010 (for 0002), or amended in place with cross-references — so that the ADR corpus tells a single coherent story.
39. As a documentation reader, I want a decision on `naamgeving-en-lagen.md` (archive, prune to cheat-sheet, annotate inline, or delete) and the chosen disposition applied, so that the document either goes away or links explicitly to the ADRs that override its formulas.
40. As a future maintainer, I want the per-entity DLT SQL files to follow one shape (tagged source MV → DW + DWH + DWQ) so that adding a new entity is a copy-rename-edit-the-business-columns exercise, not a structural redesign.
41. As a future maintainer, I want `naamgeving-en-lagen.md`'s SCD1 formulas reconciled with ADR-0012 (root `MK_`, latest-row-per-BK, `MA_CHANGEDATE` filter `WA_CRUD <> 'C'`, `MA_ISDEL` from end-dating) in whichever surviving document carries them forward, so that two competing recipes don't both ship in the repo.
42. As a future maintainer, I want `DW_<TABEL>` deletes recorded by `__END_AT`-setting (not as tombstone rows), so that the `FLOW AUTO CDC STORED AS SCD TYPE 2` mechanism doesn't have to be sidestepped — per ADR-0010's documented trade-off.
43. As a future maintainer, I want one and only one hashing algorithm in user code (`SHA2(..., 256)` per ADR-0019), so that "which hash did we use here?" never comes up during review.
44. As a future maintainer, I want `WA_HASH` computed in the tagged source MV before the `FLOW AUTO CDC` step (so DWQ rows carry it too, per ADR-0015), so that source-reconciliation queries can cover rejected rows on the same footing as cleansed rows.

## Implementation Decisions

### Layer vocabulary and identifier casing

- Staging / integration / datamart replace bronze / silver / gold everywhere — in pipeline names, comments, markdown, and prose. KRM's Ingest / Combine / Publish layer abstractions are acceptable where the abstract layer is named without a Databricks-specific word.
- Schema names stay `staging_<bron>`, `integration`, `datamart` (lowercase as per naamgeving §2.1 and CONTEXT.md §1).
- All table, view and materialised-view identifiers inside those schemas adopt the uppercase prefixed convention: `STG_<TABEL>`, `DW_<TABEL>`, `DWH_<TABEL>`, `DWQ_<TABEL>`, `DIM_<NAAM>`, `FCT_<NAAM>`. Business-key column names keep their bron-side casing where they appear unchanged; admin and surrogate columns use the prefixed uppercase convention (`SA_*`, `WA_*`, `MA_*`, `WK_`, `WKP_`, `WKR_`, `MK_`).

### Staging layer (ADR-0017 alignment for Auto Loader sources)

- The Auto Loader ingest notebook writes `SA_CRUDDTS` (`current_timestamp()`), `SA_SRC` (`source_system` from the control table) and `SA_RUNID` (the Workflow run id) instead of the previous `_ingestion_timestamp` / `_source_system` / `_source_file` / `_last_modified` / `_pipeline_run_id` set. `_source_file` and `_last_modified` are dropped — they are file-handler diagnostics, not part of the KRM admin set.
- `delta.enableChangeDataFeed = 'true'` stays on every `STG_<TABEL>` so the integration layer can read `STREAM table_changes()`.
- Schema-explicit `StructType`s in the ingest notebook keep their current `IntegerType` choice for `SHIFT_START_TIME` / `SHIFT_END_TIME` — staging stays raw, the `'HH:mm:ss'` projection happens in the integration tagged source MV.
- The two control-table rows (`order_header`, `order_detail`) get their `target_table` values rewritten in uppercase prefixed form (`STG_ORDER_HEADER`, `STG_ORDER_DETAIL`). The setup notebook's `MERGE` seed-rows are updated to match, and the seed-row predicate stays `(source_system, target_table)` so re-runs remain idempotent.
- Lakeflow Connect (`staging_sqlserver`) stays parked. Its existing target shape — native `_change_type` / `_change_version` / `_commit_timestamp` columns from the connector — is documented as the ADR-0017-compliant counterpart and left untouched.

### Integration layer (ADR-0010 + ADR-0011 + ADR-0016 alignment)

Per entity (`ORDER_HEADER`, `ORDER_DETAIL`), one DLT SQL file produces four objects:

- **Tagged source materialised view** — reads `STREAM table_changes('<staging_schema>.STG_<TABEL>')` from CDF, applies type-fixes and `SA_*` → `WA_*` admin mapping (`WA_CRUDDTS` from `current_timestamp()`, `WA_CRUD` from `_change_type` mapped to `'C' / 'U' / 'D'`, `WA_SRC` passthrough, `WA_RUNID` passthrough), computes `WA_HASH = SHA2(CONCAT_WS('||', <COALESCE'd non-BK business columns>), 256)`, and emits a `failed_rules ARRAY<STRING>` built from a `CASE`-per-rule chain stripped of `NULL`s. Carries `CONSTRAINT ... EXPECT (NOT array_contains(failed_rules, '<rule>'))` for each drop-grade rule so per-rule counts surface in the DLT event log. Filters incoming change types to `('insert', 'update_postimage', 'delete')` so update-preimages don't double-count.
- **`DW_<TABEL>` streaming table** — defined with `WK_<TABEL> BIGINT GENERATED ALWAYS AS IDENTITY`, full business-column list, the five `WA_*` admin columns, and `EXPECT (...) ON VIOLATION FAIL UPDATE` declarations for fail-grade rules (e.g. `order_id IS NOT NULL`). Populated by `FLOW AUTO CDC FROM <tagged source MV> KEYS (<BK>) SEQUENCE BY _commit_timestamp APPLY AS DELETE WHEN WA_CRUD = 'D' STORED AS SCD TYPE 2`. The clean-row predicate is `size(failed_rules) = 0`, applied in the `FLOW AUTO CDC` source projection.
- **`DWH_<TABEL>` view** — projects every column of `DW_<TABEL>`, renames `__START_AT` to `WA_FROMDATE`, `__END_AT` (COALESCE'd to `TIMESTAMP '9999-12-31 00:00:00'`) to `WA_UNTODATE`, derives `WA_ISCURR` as `CASE WHEN __END_AT IS NULL THEN 1 ELSE 0 END`, and computes `WKP_<TABEL> = LAG(WK_<TABEL>) OVER (PARTITION BY <BK> ORDER BY __START_AT)` and `WKR_<TABEL> = FIRST_VALUE(WK_<TABEL>) OVER (PARTITION BY <BK> ORDER BY __START_AT)`. No self-side BK hash (`WKR_HASH_<TABEL>`) per ADR-0014.
- **`DWQ_<TABEL>` streaming table** — populated from the same tagged source MV with `WHERE size(failed_rules) > 0`. Carries every business and `WA_*` column from the rejected change event plus `failed_rules`. No `WK_<TABEL>` IDENTITY, no `__START_AT` / `__END_AT`, append-only (no `FLOW AUTO CDC`). DWQ is allowed to retain the rejected source's `_change_type` for diagnostic purposes.

The rule severities defined in `CONTEXT.md` §7 and the previous PRD carry over unchanged in content; only the implementation surface (where each rule lives — tagged MV `CONSTRAINT`, DW `EXPECT ON VIOLATION FAIL UPDATE`, or `failed_rules` array) changes.

`INTEGRATION.SALES_LINE` becomes a plain UC view (still applied by `apply_views`) joining `DWH_ORDER_HEADER ⨝ DWH_ORDER_DETAIL`, picking the version of each entity current at the line's event timestamp using the half-open SCD2 interval `WA_FROMDATE <= order_ts < WA_UNTODATE`.

### Datamart layer (ADR-0012, ADR-0013, ADR-0018, ADR-0020 alignment)

- `DIM_DATE` becomes a plain UC view generated by `SEQUENCE(MIN(order_ts), MAX(order_ts))` exploded into one row per date, with `MK_DATE = CAST(date_format(full_date, 'yyyyMMdd') AS INT)` per ADR-0018. The existing calendar attribute list (`year`, `quarter`, `month`, `day`, `month_name`, `day_name`, `day_of_week`, `week_of_year`, `is_weekend`, `year_month_start`, `year_quarter_start`, `year_start`) is preserved.
- Entity dimensions (currently `dim_truck`, `dim_location`, `dim_customer`, `dim_menu_item`, `dim_currency`, `dim_order_channel`, `dim_shift`, `dim_discount`) are renamed and rebuilt per ADR-0012 (SCD1) or ADR-0013 (SCD2). For the current demo without master data, all entity dims default to SCD1 against `DWH_ORDER_HEADER` (or `DWH_ORDER_DETAIL` for `DIM_MENU_ITEM`), selecting the latest row per BK by `WA_FROMDATE` (not `WA_ISCURR = 1`), and exposing `MK_<TABEL> = WKR_<TABEL>` (root surrogate), `MA_CREATEDATE = MIN(WA_CRUDDTS) PARTITION BY <BK>`, `MA_CHANGEDATE = MAX(WA_CRUDDTS) FILTER (WHERE WA_CRUD <> 'C') PARTITION BY <BK>` (NULL when only the C row exists), and `MA_ISDEL = CASE WHEN <latest row's WA_UNTODATE> <> TIMESTAMP '9999-12-31 00:00:00' THEN 1 ELSE 0 END`.
- The SCD2 spec from ADR-0013 is implemented and made available for the SQL Server-unparked future; no current demo entity uses it, but the per-entity DLT SQL pattern accommodates `MK_<TABEL> = WK_<TABEL>` / `MK_ROOT = WKR_<TABEL>` / `MA_FROM` / `MA_UNTO` / `MA_ISCURR` projections.
- `FCT_ORDER` and `FCT_SALES_LINE` (still materialised views with Liquid Clustering on `FCT_SALES_LINE`) are rebuilt to read `DWH_<TABEL>` directly. The FK columns are renamed to `MK_<TABEL>` and computed as the source row's `WKR_<TABEL>` (root surrogate, stable across version churn) — not `SHA2(<natural_key>, 256)`. The half-open SCD2 interval predicate joins the fact's event timestamp to the correct entity version when entity dims are SCD2; for SCD1 entity dims the latest-row-per-BK selection in `DIM_<NAAM>` already collapses versions and the fact join becomes BK-direct.
- The `__UNKNOWN__` row pattern for NULL natural keys (warn-rule dims: truck, location, shift, discount) is preserved in semantics — the demo's "orphan rows still appear, attributed to Unknown" behaviour — but expressed through the SCD1 latest-row-per-BK selection rather than a SHA2 `COALESCE` collapse.

### DAB resources and orchestration (ADR-0020 alignment)

- `resources/dlt_integration.yml` keeps its glob include but the pipeline display name becomes `demo-integration` (Silver vocabulary removed). Similarly `resources/dlt_datamart.yml` becomes `demo-datamart`.
- `resources/demo_workflow.yml` task graph: `setup → (ingest_full ‖ ingest_incremental) → dlt_integration → (dlt_datamart ‖ apply_views)`. `dlt_datamart` no longer depends on `apply_views` (ADR-0020: facts read `DWH_` directly).
- `resources/sqlserver.yml.disabled` and `resources/sqlserver_job.yml.disabled` are inspected for stale table-name references and updated only if any of the staging-side names change in a way that affects them (none expected, but verified).
- `resources/dashboard.yml` references stay structural; the dashboard JSON itself (`dashboards/tasty_bytes_sales.lvdash.json`) is regenerated against the renamed tables.

### Documentation sweep

- `CONTEXT.md` §6 (Staging), §7 (Integration), §8 (Datamart), §9 (Mapstructuur) are rewritten against the ADR target state. §6 documents `SA_*` admin columns. §7 documents the four-object-per-entity pattern (tagged source MV, `DW_`, `DWH_`, `DWQ_`), the `STREAM table_changes()` read, SCD2 mechanism, and tagged-MV-with-`failed_rules` quarantine. §8 documents `DIM_*` view specs per ADR-0012/0013/0018 and the `FCT_*`-reads-`DWH_` topology per ADR-0020. §9 reflects the new file layout (per-entity SQL files with their four-object pattern; views folder unchanged in structure but content renamed).
- ADR-0001 is superseded explicitly by ADR-0007 + ADR-0011 (the "DLT data-quality in Silver" principle is now expressed as "route, don't drop" + "paired DWQ in integration"). ADR-0002 is amended with a "see ADR-0010 for the SCD2 mechanism" cross-reference rather than superseded — the underlying principle (Silver reads Bronze via CDF) survives in layer-renamed form (integration reads staging via CDF).
- `naamgeving-en-lagen.md` is pruned to a cheat-sheet that links to the ADRs for every formula the ADRs correct, and the body of the document is shortened to the parts that the ADR corpus does not contradict. The three ADR-0012 corrections to its §2.6 SCD1 formulas are removed from naamgeving and the ADR carries them.
- `issues/krm-adr-followups.md` is left in place during the refactor as a session-context note; once the refactor lands it is moved into `docs/archive/` or deleted.

### Deep modules (such as the SQL-first stance allows)

ADR-0009 forbids machine-readable entity registries and YAML codegen, so there are no Python helper modules to extract. The "deep module" surface here is *patterns* that appear once per entity:

- **Tagged source MV pattern** — type-fixes, `SA_*` → `WA_*` admin mapping, `WA_HASH`, `failed_rules` array, per-rule `CONSTRAINT EXPECT`. One template, varying business columns and rule list.
- **`DW_<TABEL>` + `DWH_<TABEL>` pair pattern** — IDENTITY `WK_<TABEL>`, `FLOW AUTO CDC ... STORED AS SCD TYPE 2`, then a deterministic view projection that renames validity columns and computes `WKP_` / `WKR_`. One template per entity, varying BK and business columns.
- **`DWQ_<TABEL>` sink pattern** — same source MV, inverse `failed_rules` filter, append-only ST.
- **SCD1 `DIM_<NAAM>` view pattern** — latest-row-per-BK by `WA_FROMDATE`, `WKR_` → `MK_`, `MA_CREATEDATE` / `MA_CHANGEDATE` / `MA_ISDEL` computed from window functions over `DWH_<TABEL>`.
- **`FCT_*` MV pattern** — direct read of `DWH_<TABEL>` for each dimensional source, `WKR_<TABEL>` projected as `MK_<TABEL>`, half-open SCD2 interval join where the dim is SCD2, optional `CLUSTER BY` on heavy facts.

The pattern uniformity is verified by inspection (one entity built first, the rest copied), not by automated codegen.

## Testing Decisions

**No automated tests in scope.** The repo has no `tests/` directory, no pytest configuration, and no CI test step. The previous PRD documents the same stance for the same reasons (demo template, no test infrastructure). Adding test infrastructure for a refactor whose value is "the demo continues to deploy and run" would be net-negative.

Verification is structural and manual:

- **Bundle deploy** — `databricks bundle deploy --target dev` produces all renamed objects; `databricks bundle validate` passes against the renamed resource definitions.
- **Workflow run** — one end-to-end Workflow run from `setup` through `dlt_datamart` succeeds, with row counts matching the pre-refactor counts for `DW_*` rows (modulo SCD2 versioning) and `FCT_*` rows (should match unchanged).
- **Catalog inspection** — `SHOW TABLES IN integration` lists the expected `DW_*`, `DWH_*`, `DWQ_*` set; `SHOW TABLES IN datamart` lists the expected `DIM_*`, `FCT_*` set; column lists on representative rows match the ADR-target shape.
- **DLT event log** — drop-grade rule expectations surface as per-rule violation counts.
- **Dashboard load** — the deployed Lakeview dashboard renders against the renamed tables.
- **Quarantine triage query** — at least one `DWQ_<TABEL>` row materialises (seeded by a bad input file if the source data is clean) and `WHERE array_contains(failed_rules, '<rule>')` returns it.

If the project later grows beyond demo scope, the natural automated-test targets are the per-entity tagged source MV outputs (row counts of clean vs failed, `failed_rules` correctness on representative inputs) and the SCD1 dim view formulas (latest-row-per-BK, `MA_*` computations under deletes) — both expressible as Spark SQL fixtures against tiny in-memory tables.

## Out of Scope

- **Unparking SQL Server / Lakeflow Connect.** ADR-0016 and ADR-0017 set the policy for when it unparks (canonical `DW_<TABEL>` with `WA_SRC` distinguishing; LC staging without `SA_*`), but the actual unparking has its own follow-up PRD.
- **Master data dimensions.** Customer, Truck, Menu, Location, Discount, Shift remain BK-only dims sourced from `DWH_ORDER_HEADER` / `DWH_ORDER_DETAIL` as in the current demo. Rich descriptive attributes require source-system master data and are deferred.
- **SCD2 dim views for the current demo entities.** The ADR-0013 spec is implemented in the per-entity pattern documentation but no current entity is built as SCD2 (no master data, no observed change history worth versioning). The SCD2 dim view template is left ready for SQL Server unpark.
- **AI/BI Genie space.** Genie is still configured manually post-deploy per the previous PRD; the runbook step in `docs/demo_script.md` is updated to reference the renamed `FCT_*` / `DIM_*` tables but the Genie space itself remains out of DAB scope.
- **Multi-target promotion.** `test` and `prod` DAB targets remain placeholders; the refactor's deploy story is `dev`-only.
- **Test infrastructure.** No `tests/`, no pytest, no CI test step — see Testing Decisions.
- **Delta Sharing and ML feature store integration.** As in the previous PRD, both stay deferred.
- **Compute / cost optimisation.** Pipeline cluster autoscale settings stay at their current values; tuning is out of scope.
- **ADR-0001 / ADR-0002 wording polish beyond supersede / amend.** The two ADRs are reconciled minimally (one `superseded by:` block, one `amended by:` block); broader rewriting is deferred.
- **Renaming the catalog or schemas.** The catalog stays `DEMO`; schemas stay `staging_*` / `integration` / `datamart` (lowercase). Only objects inside those schemas adopt the uppercase prefixed convention.

## Further Notes

- The change set is binding-by-ADR but non-trivial in surface area: every per-entity DLT SQL file, every dim view notebook, both fact MVs, the staging ingest notebook, the setup notebook's control-table seed rows, the dashboard JSON, the four resource YAMLs, three CONTEXT.md sections, ADRs 0001 and 0002, and `naamgeving-en-lagen.md` all change. The vertical-slice ordering proposed in `issues/krm-adr-followups.md` Follow-up C — rename, then SCD2, then DWH, then DWQ, then dims, then facts, then DAB — is one viable issue breakdown. A per-entity slice (do `ORDER_HEADER` end-to-end first as the canonical pattern, then `ORDER_DETAIL` against the established template) is the alternative.
- `issues/krm-adr-followups.md` already pre-decomposes the work into Follow-ups A (docs), B (ADR housekeeping) and C (code refactor). This PRD restates Follow-ups A and C as the primary scope and folds Follow-up B's ADR-0001 supersede / ADR-0002 amend / naamgeving disposition into the documentation sweep. Once approved, `/to-issues` against this PRD produces the per-PR breakdown on `AOEshun/databricks-demo`.
- ADRs 0003–0020 are the source of truth for every decision in this PRD. Where this PRD restates an ADR, the ADR governs on disagreement.
- The previous `issues/prd.md` (the original "build Silver/Gold" PRD) has been delivered and is superseded by this file. The implementation it described is the starting point this refactor edits forward; the related history is recoverable via `git log issues/prd.md`.
- ADR-0020's note that staging Auto Loader and dim views are explicit deviations from naamgeving §1 ("alles draait in Lakeflow Declarative Pipelines op serverless compute") stays as documented — the refactor does not migrate those to DLT.
