# KRM/ADR refactor — follow-ups

## Session context (so a fresh session can pick this up cold)

In a previous session we adopted KRM (Kadenza Referentie Methodiek) as the binding architectural convention for this Databricks demo, captured in `krm_source/KRM Samenvatting.pptx`. The naming/layering doc `naamgeving-en-lagen.md` was reviewed against KRM and **18 new ADRs (0003–0020)** were written into `docs/adr/`, plus a scope amendment to ADR-0004. ADR-0001 and ADR-0002 were left in place but are now partly stale (Silver/Bronze vocabulary, SCD1 mechanism).

Key decisions recorded:

- KRM principles SQL-first, metadata-driven, end-to-end lineage, shared-logic-in-views, route-don't-drop, one-time ingestion (ADR-0003–0008)
- Entity structure lives in SQL (no YAML codegen, no entity-registry control table) (ADR-0009)
- DW via `APPLY CHANGES INTO STORED AS SCD TYPE 2` reading `STREAM table_changes()` over staging CDF (ADR-0010)
- Quarantine: paired `DWQ_<TABEL>` from a tagged source MV with `failed_rules ARRAY<STRING>`; rule logic appears once (ADR-0011)
- SCD1 dim corrections (root MK_, latest-row-per-BK, MA_CHANGEDATE includes deletes, MA_ISDEL from end-dating) (ADR-0012)
- SCD2 dim spec pinned (ADR-0013)
- No self-side BK hash; FK-side `WK_REF_HASH_<REF>` stays (ADR-0014)
- WA_HASH kept narrowly for source reconciliation (ADR-0015)
- Integration tables are canonical, not per-source — WA_SRC distinguishes provenance (ADR-0016)
- Lakeflow Connect staging carries native CDC columns, no `SA_*` (ADR-0017)
- DIM_DATE as a generated calendar view, MK_DATE = yyyymmdd INT (ADR-0018)
- All hashes are SHA2-256 (ADR-0019)
- Topology: one Workflow, dlt_integration + dlt_datamart split, dims as plain UC views via apply_views, facts read DWH_ directly (ADR-0020)

Refactor implication: the current codebase does **not** follow these conventions yet (lowercase snake_case names, SHA2 surrogates, SCD1 via APPLY CHANGES FROM SNAPSHOT, `_quarantine` suffix naming, etc.). The ADRs are binding, so the code needs to catch up.

---

## Follow-up A — Documentation sweep (`CONTEXT.md`)

1. **§7 wholesale rewrite** — current text describes `APPLY CHANGES FROM SNAPSHOT` SCD1 + `_quarantine` suffix tables. Replace with: SCD2 + CDF reads via `STREAM table_changes()`, DW/DWH twin pattern, `DWQ_` naming, tagged-MV-with-failed_rules mechanism per ADR-0010 and ADR-0011.

2. **§8 datamart-dim rewrite** — SCD1 corrections per ADR-0012 (root MK_ from WKR_, latest-row-per-BK source, MA_CHANGEDATE includes deletes, MA_ISDEL via end-dating); SCD2 spec per ADR-0013; remove `WHERE WA_ISCURR=1` references where they no longer apply.

3. **§6 + §9 topology updates** — reflect ADR-0020: `dlt_integration` and `dlt_datamart` stay split; `apply_views` survives and produces DIM_ as plain UC views; facts read `DWH_` directly, not `DIM_`.

4. **Normalise tabelspec sections** — §7 (integration) and §8 (datamart) currently mix shapes. Pick one per-entity table shape and apply it consistently. Decide whether staging entities deserve tabelspec entries (currently they don't).

5. **Vocabulary sweep** — purge Silver/Bronze/Gold from the corpus in favour of integration/staging/datamart (or KRM's Ingest/Combine/Publish where the abstract layer name is needed). Implicates ADR-0001 and ADR-0002.

## Follow-up B — ADR corpus housekeeping

6. **Supersede ADR-0001** ("DLT en data-quality-checks horen in Silver, niet in Bronze") — the principle stands but is now better expressed by ADR-0007 (route, don't drop) + ADR-0011 (paired DWQ_ in integration, not staging). Either explicit supersede, or amend to update vocabulary.

7. **Amend or supersede ADR-0002** ("Silver leest Bronze via Change Data Feed + apply_changes") — broadly compatible with ADR-0010 but uses Silver/Bronze vocabulary. Likely amend with a note pointing to ADR-0010 for the specific SCD2 mechanism.

8. **Naamgeving disposition** — `naamgeving-en-lagen.md` now has multiple corrections embedded in the new ADRs. Decide:
   - (a) archive it (move to `docs/archive/`, keep for history)
   - (b) prune it to a quick-reference cheat-sheet that links to ADRs
   - (c) annotate it inline with the corrections and ADR cross-references
   - (d) delete it — the ADRs are the source of truth now

## Follow-up C — Code refactor (the binding work)

This is a substantial multi-PR effort. The /to-issues skill can break it into independent issues; suggested vertical-slice ordering:

9. **Rename schemas and tables to KRM HOOFDLETTERS** — schemas keep their names (`staging_*`, `integration`, `datamart`) but tables become `STG_*`, `DW_*`, `DWH_*`, `DIM_*`, `FCT_*`, `DWQ_*`. Coordinated rename across all SQL files, notebooks, DAB resource definitions, and the AI/BI dashboard JSON.

10. **Migrate `integration/*.sql` to SCD2 via APPLY CHANGES INTO** (per ADR-0010) — replace `APPLY CHANGES FROM SNAPSHOT` reads with `STREAM table_changes()`-from-staging-CDF reads. Add `WA_*` admin columns, drop the `_clean_src` intermediate-MV pattern.

11. **Add DW_<TABEL> IDENTITY surrogates + paired DWH_<TABEL> views** — DW gets `WK_<TABEL> BIGINT GENERATED ALWAYS AS IDENTITY`, DWH view computes `WKP_` / `WKR_` via window functions, renames `__START_AT`/`__END_AT` to `WA_FROMDATE`/`WA_UNTODATE`/`WA_ISCURR`.

12. **Refactor quarantine to DWQ_ tagged-MV pattern** (per ADR-0011) — rename `_quarantine` → `DWQ_`, restructure to the tagged-MV-with-failed_rules pattern, add `CONSTRAINT ... EXPECT (NOT array_contains(failed_rules, '...'))` for DLT event-log metrics.

13. **Rewrite dim view notebooks** (per ADR-0012, ADR-0013, ADR-0018) — new `MK_`/`MA_` admin columns; SCD1 latest-row-per-BK source; SCD2 expose-every-version; DIM_DATE as generated calendar with yyyymmdd MK_DATE.

14. **Rebuild facts to read DWH_ directly** (per ADR-0020) — `FCT_*` MVs join `DWH_*` (not `DIM_*`) and project `WKR_<TABEL>` as their `MK_<TABEL>` FK with the half-open temporal interval for SCD2.

15. **Update DAB resources** — `databricks.yml`, `resources/demo_workflow.yml`, `resources/dlt_integration.yml`, `resources/dlt_datamart.yml`, `resources/dashboard.yml` all need to reflect renamed tables and the revised task graph.

---

## Recommended starting point in the next session

Suggested order: **A → B → C** (docs first, then ADR housekeeping, then code refactor).

1. Start with **A** so CONTEXT.md and the ADR corpus tell a coherent story before touching code. The doc sweep also surfaces any contradiction between ADRs and what's *actually* recorded, catching ADR bugs cheaply.
2. **B** is a small follow-on — supersede 0001, amend 0002, decide naamgeving disposition.
3. **C** is the real refactor. Use `/to-issues` to break it into issues on `AOEshun/databricks-demo` (this repo uses GitHub Issues per `CLAUDE.md`). The vertical-slice ordering above is one option; another is per-entity (do the full refactor for one entity first as a pattern, then replicate).

Open `docs/adr/0003-…` through `docs/adr/0020-…` to remind yourself of the binding decisions. `naamgeving-en-lagen.md` is no longer authoritative — defer to ADRs on any conflict.
