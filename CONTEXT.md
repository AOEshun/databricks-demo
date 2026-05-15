# Databricks Demo Template — Architectuurbeslissingen

## Context

Dit document beschrijft alle architectuurbeslissingen voor een Databricks demo-omgeving.
De demo is bedoeld om de kracht en best practices van het Databricks-platform te tonen aan klanten.
De omgeving moet productie-waardig zijn in opzet en uitleg, ook al is het primair een demo.

---

## 1. Unity Catalog Structuur

### Catalog
Eén catalog `DEMO`, dezelfde naam in elke omgeving. Omgevingsisolatie loopt via de Databricks Asset Bundle target (dev/test/prod) en de bijbehorende workspace — niet via catalog-naam.

| Catalog | Doel |
|---|---|
| `DEMO` | Demo-objecten — dev/test/prod onderscheid via DAB target |

**Argumentatie:** Workspace- en target-isolatie geven al sterke scheiding; één catalog-naam houdt de demo-code en -documentatie consistent over omgevingen heen en voorkomt drift in SQL-snippets.

### Schema's (binnen elke catalog)
Staging-schema's zijn per bron (zodat laad-semantiek apart blijft); integration en datamart zijn integraal — daar verdwijnt het bron-onderscheid bewust (zie ADR-0016).

| Schema | Laag (KRM) | Inhoud |
|---|---|---|
| `CONFIG` | Configuratie | Control table die de parquet-pipeline aanstuurt |
| `staging_azurestorage` | Staging / Ingest | Parquet-bronnen uit Azure Storage Container (`STG_*` tabellen) |
| `staging_sqlserver` | Staging / Ingest | SQL Server bron via Lakeflow Connect (geparkeerd, `STG_*` tabellen) |
| `integration` | Integration / Combine | `DW_*` / `DWH_*` / `DWQ_*` per entiteit + integration views |
| `datamart` | Datamart / Publish | `DIM_*` views + `FCT_*` materialised views voor AI/BI |

---

## 2. Bronnen

| Bron | Type | Inhoud |
|---|---|---|
| Azure Storage Container | Parquet-bestanden | `order_header`, `order_detail` |
| Azure SQL Server | SQL Database | 1 tabel *(naam nog onbekend)* |

---

## 3. Volumes

- **Één External Volume** op container-niveau, gekoppeld aan de Azure Storage Container
- De parquet-bestanden staan plat in één `source/` map, met bestandsnamen die met `ORDER_HEADER` of `ORDER_DETAIL` beginnen
- Routering naar de juiste doeltabel gebeurt via Auto Loader's `pathGlobFilter`, gestuurd door de `file_pattern` kolom in de control table

```
/Volumes/demo/staging_azurestorage/parquet/
├── ORDER_HEADER*.parquet
├── ORDER_DETAIL*.parquet
└── _checkpoints/        # Auto Loader checkpoints per target table
```

---

## 4. Control Table

### Locatie
```
DEMO.CONFIG.pipeline_sources
```

### Kolommen

| Kolom | Type | Voorbeeld | Doel |
|---|---|---|---|
| `source_system` | string | `azurestorage` | Welk bronsysteem |
| `source_path` | string | `/Volumes/demo/staging_azurestorage/parquet` | Pad naar de bronfolder |
| `file_pattern` | string | `ORDER_HEADER*.parquet` | Glob filter binnen de folder (per doeltabel) |
| `target_schema` | string | `staging_azurestorage` | Doelschema |
| `target_table` | string | `STG_ORDER_HEADER` | Doeltabelnaam |
| `file_format` | string | `parquet` | Bestandstype |
| `is_active` | boolean | `true` | Aan/uit zonder rij te verwijderen |
| `load_type` | string | `full` | `full` of `incremental` (Auto Loader) |

> **Opmerking:** Alleen de parquet-pipeline leest deze control table. SQL Server wordt via Lakeflow Connect ingeladen en verschijnt niet in de control table.

### Initiële vulling

| source_system | source_path | file_pattern | target_table | load_type |
|---|---|---|---|---|
| `azurestorage` | `/Volumes/demo/staging_azurestorage/parquet` | `ORDER_HEADER*.parquet` | `STG_ORDER_HEADER` | `full` |
| `azurestorage` | `/Volumes/demo/staging_azurestorage/parquet` | `ORDER_DETAIL*.parquet` | `STG_ORDER_DETAIL` | `full` |

**Argumentatie:** De control table is een Delta-tabel en profiteert daarmee automatisch van Delta Time Travel, Unity Catalog Audit Logs en Lineage — zonder extra configuratie.

---

## 5. Audit-kolommen (per bron verschillend)

Audit-kolommen worden gesplitst per bron, omdat Lakeflow Connect zijn eigen doeltabellen beheert en geen ruimte biedt voor custom kolommen — terwijl het wel rijkere CDC-metadata levert dan we zelf zouden toevoegen. Zie §6 voor de volledige `SA_*` admin-set en ADR-0017 voor de architectuurkeuze.

### Azure Storage staging-tabellen (parquet)

Drie `SA_*`-kolommen worden toegevoegd aan elke `STG_*`-doeltabel (zie §6):

| Kolom | Inhoud |
|---|---|
| `SA_CRUDDTS` | `current_timestamp()` — moment waarop de rij in staging arriveerde |
| `SA_SRC` | Bronsysteem-tag (bijv. `'azurestorage'`) |
| `SA_RUNID` | Databricks job run id |

### SQL Server staging-tabellen (Lakeflow Connect)

Geen `SA_*`-kolommen — Lakeflow Connect beheert het tabel-schema en levert in plaats daarvan zijn eigen CDC-metadata (ADR-0017):

| Kolom | Inhoud |
|---|---|
| `_change_type` | `insert`, `update_preimage`, `update_postimage`, `delete` |
| `_change_version` | Monotone CDC-versie van de bron |
| `_commit_timestamp` | Tijdstip van de wijziging in de bron |

> **Demo-waarde:** `_change_type` is een sterker demo-moment dan een eigen `SA_CRUDDTS` — klanten zien letterlijk welke rijen gewijzigd, ingevoegd of verwijderd zijn.

---

## 6. Pipeline Aanpak — Staging (Ingest)

### Doel van de laag

Staging is de KRM "Ingest"-laag: elke externe bron wordt **één keer** in de lakehouse vastgelegd (ADR-0008), zo dicht mogelijk bij het bronformaat. Geen type-casts, geen kwaliteitsregels, geen quarantine — die horen pas in `integration` (ADR-0001 herzien door ADR-0007 + ADR-0011, zie §7). De integration-laag leest exclusief de staging-tabellen, nooit terug naar de bron.

### Tabelnaamgeving — `STG_<TABEL>`

Elke staging-tabel heet `STG_<TABEL>` (hoofdletters), in een per-bron schema:

| Schema | Tabellen | Bron |
|---|---|---|
| `staging_azurestorage` | `STG_ORDER_HEADER`, `STG_ORDER_DETAIL` | Auto Loader op parquet |
| `staging_sqlserver` | `STG_<TABEL>` (per LC-tabel) | Lakeflow Connect (geparkeerd) |

### Eén notebook, drie modes (Auto Loader-bronnen)

`staging/02_ingest_azurestorage.ipynb` is één notebook met een `mode` widget. De control table (ADR-0004) bepaalt **welke** tabel in welke mode hoort; het notebook doet alleen de mode die zijn widget zegt.

| `mode` | Gedrag |
|---|---|
| `full` | Filter control table op `load_type='full'`, overschrijf elke doeltabel volledig |
| `incremental` | Filter control table op `load_type='incremental'`, Auto Loader (`cloudFiles`) met checkpoint per doeltabel |
| `both` | Beide bovenstaande achtereenvolgens — handig voor ad-hoc runs vanuit de notebook-UI |

### Demo-moment: live mode-switch

Eén UPDATE in de control table verandert het gedrag — geen codewijziging nodig:

```sql
UPDATE DEMO.CONFIG.pipeline_sources
SET    load_type = 'incremental'
WHERE  source_system = 'azurestorage'
AND    target_table  = 'STG_ORDER_HEADER'
```

Daarna verwerkt `mode=incremental` deze rij wél; `mode=full` slaat hem over.

### Reset widget — bij switch op een gevulde tabel

Wanneer een tabel die al via `full` is geladen wordt omgezet naar `incremental`, dupliceert de eerste incremental-run alle bestaande rijen: Auto Loader heeft geen checkpoint en behandelt elk bronbestand als nieuw.

Het notebook heeft daarom een `reset` widget (default `false`). Op `true`:
1. Drop de doeltabel
2. Verwijder de checkpoint-folder onder `_checkpoints/{target_table}/`
3. Auto Loader start schoon

De presentator zet `reset=true` expliciet bij de eerste incremental-run na een mode-switch — geen verborgen magie.

### Admin-kolommen — `SA_*` per ADR-0017

Voor **Auto Loader-bronnen** voegt de ingest-laag drie admin-kolommen toe aan elke `STG_*`-tabel. Dit vervangt de eerdere vijfvoudige set (`_ingestion_timestamp` / `_source_system` / `_source_file` / `_last_modified` / `_pipeline_run_id`). `_source_file` en `_last_modified` waren file-handler diagnostiek en horen niet bij de KRM admin-set.

| Kolom | Type | Inhoud |
|---|---|---|
| `SA_CRUDDTS` | TIMESTAMP | `current_timestamp()` — moment waarop de rij in staging arriveerde |
| `SA_SRC` | STRING | Bronsysteem-tag (bijv. `'azurestorage'`) |
| `SA_RUNID` | STRING | Databricks job run id |

Voor **Lakeflow Connect-bronnen** worden geen `SA_*`-kolommen toegevoegd (ADR-0017). LC beheert het tabel-schema en is niet uitbreidbaar; in plaats daarvan levert LC zijn eigen CDC-metadata:

| Kolom | Inhoud |
|---|---|
| `_change_type` | `insert`, `update_preimage`, `update_postimage`, `delete` |
| `_change_version` | Monotone CDC-versie van de bron |
| `_commit_timestamp` | Tijdstip van de wijziging in de bron |

De integration-laag past zich per bron aan: voor Auto Loader-entiteiten mapt het `SA_CRUDDTS` → `WA_CRUDDTS` en gebruikt het staging-CDF voor sequencing; voor LC-entiteiten mapt het `_change_type` → `WA_CRUD` en gebruikt het `_commit_timestamp` direct. Beide produceren dezelfde `DW_`-vorm — zie §7.

### Change Data Feed aan op elke `STG_*`-tabel

Elke `STG_*`-tabel wordt aangemaakt met `TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')`. De integration-laag leest via `STREAM table_changes('staging_azurestorage.STG_<TABEL>', 1)` zodat zowel inserts als overschrijvingen als CDF-events binnenkomen — zie §7 ("Bron-leespatroon").

### Workflow

`resources/demo_workflow.yml` definieert één Workflow; de ingest-stap heeft twee parallelle taken:

```
setup
  ├─→ ingest_azurestorage_full         (mode=full,        parallel)
  └─→ ingest_azurestorage_incremental  (mode=incremental, parallel)
```

Beide ingest-taken wijzen naar **dezelfde** notebook met verschillende `mode`-parameters. Een taak waarvan de control-table-filter nul rijen oplevert is een no-op.

Voor demo-momenten waarin je gericht één mode wilt draaien: open het notebook in de Databricks UI, zet de `mode`-widget en klik Run.

---

## 7. Pipeline Aanpak — Integration (Combine)

### Doel van de laag

Integration is de KRM "Combine"-laag: per bedrijfsentiteit één canonieke `DW_<TABEL>` (ADR-0016) waarop de hele historie via SCD2 is vastgelegd, met gepaarde `DWQ_<TABEL>` voor afgekeurde rijen en een `DWH_<TABEL>`-view voor lineage-derivaties. Geen per-bron tabelnamen — `WA_SRC` onderscheidt provenance row-by-row.

### Vier-objecten-per-entiteit patroon

Per entiteit declareert één `integration/<entiteit>.sql`-bestand vier objecten in het `integration`-schema (ADR-0010 + ADR-0011 + ADR-0016). De DLT-pipeline `dlt_integration` pikt deze bestanden op via `libraries: - glob: include: integration/*.sql` in `resources/dlt_integration.yml`. Daarnaast bestaat `integration.SALES_LINE` als plain Unity Catalog view in een eigen notebook (`views/integration/sales_line.ipynb`), aangeroepen door de `apply_views`-orchestrator.

| Object | Type | Beheerd door | Rol |
|---|---|---|---|
| `<entity>_src` | Materialised View (tagged source) | DLT-pipeline | Type-fixes, `SA_*`→`WA_*`-mapping, `WA_HASH`, `failed_rules ARRAY<STRING>`; per-rule `EXPECT`-constraints |
| `DW_<TABEL>` | Streaming Table (SCD2) | DLT-pipeline | Historische storage; `FLOW AUTO CDC ... STORED AS SCD TYPE 2`; gefilterd op `size(failed_rules)=0` |
| `DWH_<TABEL>` | View | DLT-pipeline | Renaming + window-functions: `WA_FROMDATE`/`WA_UNTODATE`/`WA_ISCURR`/`WKP_*`/`WKR_*` |
| `DWQ_<TABEL>` | Streaming Table (append-only) | DLT-pipeline | Afgekeurde rijen; gefilterd op `size(failed_rules)>0`; volledige `WA_*` + `failed_rules` + `_change_type` |
| `integration.SALES_LINE` | View | `views/integration/sales_line.ipynb` (via `apply_views`) | Half-open SCD2-join `DWH_ORDER_HEADER ⨝ DWH_ORDER_DETAIL` |

### 1. Tagged source MV — `<entity>_src`

Leest het CDF van de staging-tabel via `STREAM table_changes('staging_azurestorage.STG_<TABEL>', 1)`, filtert op `_change_type IN ('insert','update_postimage','delete')` om pre-image-rijen weg te gooien, doet type-fixes, mapt `SA_*` → `WA_*` admin (inclusief `WA_CRUD` afgeleid van `_change_type`), berekent `WA_HASH` (SHA2-256) over alle non-BK business-kolommen, en bouwt `failed_rules ARRAY<STRING>` via CASE-per-rule. Per drop-grade-regel staat één `CONSTRAINT ... EXPECT (NOT array_contains(failed_rules, '<rule>'))` zodat per-rule schendings-tellingen in het DLT event log verschijnen.

**Belangrijke mapping:** `WA_CRUDDTS ← SA_CRUDDTS` (het staging-ingest-moment, ADR-0017) — **niet** `current_timestamp()`. `current_timestamp()` zou de integratie-verwerkingstijd vastleggen in plaats van de bron-ingest-tijd, wat een misleidende audit-trail oplevert.

Voor Lakeflow Connect-bronnen leest dezelfde MV-vorm direct uit de LC-staging tabel en mapt `_change_type` → `WA_CRUD`; geen `SA_*`-mapping want LC levert die niet (ADR-0017). De bron-specifieke vertaling zit dus volledig in dit ene bestand (ADR-0009).

```sql
CREATE OR REFRESH STREAMING TABLE order_header_src
  CONSTRAINT order_ts_not_null       EXPECT (NOT array_contains(failed_rules, 'order_ts_not_null'))
  CONSTRAINT customer_id_not_null    EXPECT (NOT array_contains(failed_rules, 'customer_id_not_null'))
  CONSTRAINT order_total_non_negative EXPECT (NOT array_contains(failed_rules, 'order_total_non_negative'))
  -- … één CONSTRAINT per drop-regel …
AS
SELECT
  -- BK + business-kolommen (met type-fixes)
  order_id,
  CAST(order_ts AS TIMESTAMP)                                       AS order_ts,
  date_format(from_unixtime(shift_start_time/1000), 'HH:mm:ss')     AS shift_start_time,
  -- … overige business-kolommen …
  -- WA_* admin
  SA_CRUDDTS                                                        AS WA_CRUDDTS,
  SA_SRC                                                            AS WA_SRC,
  SA_RUNID                                                          AS WA_RUNID,
  CASE _change_type WHEN 'insert' THEN 'C'
                    WHEN 'update_postimage' THEN 'U'
                    WHEN 'delete' THEN 'D' END                       AS WA_CRUD,
  _commit_timestamp,
  _change_type,
  -- WA_HASH (ADR-0015 + ADR-0019) — alle non-BK business-kolommen, SHA2-256
  SHA2(CONCAT_WS('||',
    COALESCE(CAST(order_ts AS STRING), ''),
    COALESCE(CAST(customer_id AS STRING), ''),
    -- …
  ), 256)                                                            AS WA_HASH,
  -- failed_rules — CASE per regel, NULL voor passerende rijen
  array_except(
    array(
      CASE WHEN order_ts IS NULL    THEN 'order_ts_not_null'        END,
      CASE WHEN customer_id IS NULL THEN 'customer_id_not_null'     END,
      CASE WHEN order_total < 0     THEN 'order_total_non_negative' END
      -- …
    ),
    array(CAST(NULL AS STRING))
  )                                                                  AS failed_rules
FROM STREAM table_changes('staging_azurestorage.STG_ORDER_HEADER', 1)
WHERE _change_type IN ('insert', 'update_postimage', 'delete');
```

### 2. `DW_<TABEL>` Streaming Table — SCD2 storage

Streaming Table met een `WK_<TABEL> BIGINT GENERATED ALWAYS AS IDENTITY`-surrogate (versie-niveau), populated via `FLOW AUTO CDC ... STORED AS SCD TYPE 2` (ADR-0010). Databricks beheert `__START_AT` en `__END_AT` automatisch; deletes end-daten de vorige current-rij, geen tombstone-rij (ADR-0010 trade-off geaccepteerd). Schema-niveau-invarianten staan als fail-grade Expectation.

```sql
CREATE OR REFRESH STREAMING TABLE DW_ORDER_HEADER (
  WK_ORDER_HEADER BIGINT GENERATED ALWAYS AS IDENTITY,
  order_id        DECIMAL(38,0) NOT NULL,
  -- … business-kolommen + WA_* admin …
  CONSTRAINT order_id_not_null EXPECT (order_id IS NOT NULL) ON VIOLATION FAIL UPDATE
)
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
FLOW AUTO CDC
FROM (SELECT * FROM STREAM(order_header_src) WHERE size(failed_rules) = 0)
KEYS (order_id)
SEQUENCE BY _commit_timestamp
APPLY AS DELETE WHEN WA_CRUD = 'D'
STORED AS SCD TYPE 2;
```

### 3. `DWH_<TABEL>` view — lineage-derivaties

Hernoemt Databricks' `__START_AT` / `__END_AT` naar `WA_FROMDATE` / `WA_UNTODATE` (ADR-0010), COALESCE't `__END_AT` op `TIMESTAMP '9999-12-31 00:00:00'`, derivt `WA_ISCURR`, en berekent twee window-function surrogates (ADR-0006: gedeelde logica leeft in views):

| Kolom | Berekening |
|---|---|
| `WA_FROMDATE` | `__START_AT` |
| `WA_UNTODATE` | `COALESCE(__END_AT, TIMESTAMP '9999-12-31 00:00:00')` |
| `WA_ISCURR` | `CASE WHEN __END_AT IS NULL THEN 1 ELSE 0 END` |
| `WKP_<TABEL>` | `LAG(WK_<TABEL>) OVER (PARTITION BY <BK> ORDER BY __START_AT)` — vorige versie |
| `WKR_<TABEL>` | `FIRST_VALUE(WK_<TABEL>) OVER (PARTITION BY <BK> ORDER BY __START_AT)` — root (eerste ooit) |

De `DWH_<TABEL>` view draagt **geen** `WKR_HASH_<TABEL>` zelf-zijde-hash (ADR-0014): het demo-catalog is consistent over targets en de natuurlijke BK is al een passthrough-kolom.

### 4. `DWQ_<TABEL>` quarantine Streaming Table

Append-only Streaming Table die uit dezelfde tagged source MV leest, gefilterd op `size(failed_rules) > 0`. Draagt alle business-kolommen + volledige `WA_*` admin + `failed_rules` + `_change_type` (diagnostiek). Geen `WK_`-surrogate, geen `__START_AT`/`__END_AT` — quarantine is geen SCD2.

Triage:

```sql
SELECT * FROM integration.DWQ_ORDER_HEADER
WHERE  array_contains(failed_rules, 'order_total_non_negative');
```

### Bron-leespatroon: `STREAM table_changes()` (ADR-0010)

`STG_*`-tabellen hebben CDF aan; de tagged source MV consumeert dat CDF via `STREAM table_changes('staging_<bron>.STG_<TABEL>', 1)`. Effect:
- Full overschrijvingen in staging: ontbrekende rijen verschijnen als `_change_type = 'delete'`, nieuwe als `'insert'`, gewijzigde als `'update_preimage' + 'update_postimage'`.
- Incremental appends: alleen nieuwe rijen als `'insert'`.
- `FLOW AUTO CDC ... STORED AS SCD TYPE 2 SEQUENCE BY _commit_timestamp APPLY AS DELETE WHEN WA_CRUD='D'` doet de declaratieve MERGE.

### Quarantine-patroon (single-source-of-truth per entiteit)

ADR-0011 vereist dat de regel-logica per entiteit **op één plek** leeft. Het oude gepaarde `_quarantine.sql`-bestand verdwijnt: de tagged source MV bouwt `failed_rules`, en zowel `DW_<TABEL>` (`size(failed_rules)=0`) als `DWQ_<TABEL>` (`size(failed_rules)>0`) lezen daaruit. Eén kopie van de regel-CASE-statements, twee consumenten. Per-rule `CONSTRAINT EXPECT`-declaraties produceren per-rule violation-counts in de DLT event log.

### Drie ernstniveaus

| Niveau | Mechanisme in SQL DLT | Effect |
|---|---|---|
| `warn` | `CONSTRAINT … EXPECT (…)` op de tagged MV zonder `ON VIOLATION` | Rij blijft in `DW_`; schending logged in event log |
| `drop` | CASE in `failed_rules` + `CONSTRAINT EXPECT (NOT array_contains(failed_rules, '<rule>'))` op tagged MV | Rij gaat naar `DWQ_<TABEL>`; per-rule violation-count in event log |
| `fail` | `EXPECT (…) ON VIOLATION FAIL UPDATE` op de `DW_<TABEL>`-declaratie | Pipeline halt op schending |

### Regel-set per entiteit

**`DW_ORDER_HEADER`** (BK: `order_id`):

| Regel | Niveau |
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

**`DW_ORDER_DETAIL`** (BK: `order_detail_id`):

| Regel | Niveau |
|---|---|
| `order_detail_id IS NOT NULL` | fail |
| `order_id IS NOT NULL` | drop |
| `menu_item_id IS NOT NULL` | drop |
| `quantity > 0` | drop |
| `unit_price >= 0` | drop |
| `price >= 0` | drop |
| `line_number > 0` | warn |

### Type-fixes (Staging → Integration)

Spark/Delta heeft geen native time-of-day type — de staging int-millis wordt daarom een `'HH:mm:ss'`-string in integration (meest leesbaar; geen verwarrende `1970-01-01` placeholder-datum). De type-fixes leven in de tagged source MV.

| Staging-kolom | Staging-type | Integration-type | Notitie |
|---|---|---|---|
| `SERVED_TS` | `StringType` | `TimestampType` | Parse `yyyy-MM-dd HH:mm:ss` |
| `ORDER_TAX_AMOUNT` | `StringType` | `DecimalType(38, 4)` | |
| `ORDER_DISCOUNT_AMOUNT` | `StringType` | `DecimalType(38, 4)` | |
| `ORDER_ITEM_DISCOUNT_AMOUNT` | `StringType` | `DecimalType(38, 4)` | |
| `LOCATION_ID` | `DoubleType` | `DecimalType(38, 0)` | IDs zijn geen Doubles |
| `DISCOUNT_ID` | `StringType` | `DecimalType(38, 0)` (nullable) | indien numeriek in bron |
| `SHIFT_START_TIME` | `IntegerType` (millis) | `StringType` `'HH:mm:ss'` | Staging blijft de audit-trail van de raw int-millis |
| `SHIFT_END_TIME` | `IntegerType` (millis) | `StringType` `'HH:mm:ss'` | Idem |

### `WA_HASH` — reconciliatie, geen change-detection (ADR-0015)

Elke `DW_`-rij draagt `WA_HASH = SHA2(CONCAT_WS('||', <non-BK business kolommen>), 256)` (ADR-0015 + ADR-0019). Berekend in de tagged source MV — quarantine-rijen dragen het dus ook. Doel: bron-systeem-reconciliatie en forensische signature, **niet** change-detection (`FLOW AUTO CDC` doet zelf al kolom-niveau-vergelijking).

### Workflow-integratie

```
setup
  ├─→ ingest_azurestorage_full         (mode=full)
  └─→ ingest_azurestorage_incremental  (mode=incremental)
       ↓
       dlt_integration                  (DLT pipeline_task — tagged MVs + DW_ + DWH_ + DWQ_ per entiteit)
        ├─→ dlt_datamart                (FCT_* MVs lezen DWH_* direct)
        └─→ apply_views                 (DIM_* views, SALES_LINE view)
```

`dlt_integration` hangt af van **beide** ingest-taken. Per ADR-0020 lezen feiten direct uit `DWH_<TABEL>`, dus `dlt_datamart` en `apply_views` mogen parallel na `dlt_integration` draaien.

### Wat de integration-laag specifiek demonstreert

- **`FLOW AUTO CDC ... STORED AS SCD TYPE 2`** voor declaratieve historie-opbouw — pure SQL, geen handmatige MERGE-statements
- **Eén tagged source MV** met `failed_rules ARRAY<STRING>` als single-source-of-truth voor per-entiteit kwaliteitsregels
- **Per-rule `EXPECT`-constraints** geven per-rule violation-counts in het DLT event log
- **Gepaarde `DWQ_<TABEL>`** met volledige WA-context — demo-tijdse triage via `array_contains(failed_rules, '…')`
- **Declaratieve graph view** in de DLT-UI: tagged MV → `DW_` ST → `DWH_` view + `DWQ_` ST per entiteit
- **Lakeflow Connect-asymmetrie**: dezelfde `DW_`-vorm uit een LC-bron, per-bron-verschil contained in één SQL-bestand

---

## 8. Pipeline Aanpak — Datamart (Publish, Star Schema)

### Doel van de laag

Datamart is de KRM "Publish"-laag: consumption-ready, project-specifieke data in een klassieke Kimball **star schema** vorm. `DIM_<NAAM>` views projecteren op `DWH_<TABEL>`; `FCT_<NAAM>` materialised views lezen `DWH_<TABEL>` direct (ADR-0020) — geen tussentijdse dim-lookup nodig. Per Databricks-best-practice: "de-normalized and read-optimized data models" + Materialised Views voor declaratieve, auto-refreshing aggregaten.

### Hybride architectuur — dims als views, facts als MVs

Per ADR-0020 zijn dims plain Unity Catalog views (zero-storage, altijd vers, queryable door dashboards en Genie); feiten zijn DLT Materialised Views in de `dlt_datamart` pipeline.

| Object | Type | Beheerd door | Bron |
|---|---|---|---|
| `datamart.DIM_DATE` | View | `apply_views` | Gegenereerd via SEQUENCE+EXPLODE uit fact-bereik (ADR-0018) |
| `datamart.DIM_TRUCK` … `DIM_DISCOUNT` (header-zijde) | View (SCD1) | `apply_views` | `DWH_ORDER_HEADER` — latest-row-per-BK (ADR-0012) |
| `datamart.DIM_MENU_ITEM` | View (SCD1) | `apply_views` | `DWH_ORDER_DETAIL` — latest-row-per-BK (ADR-0012) |
| `datamart.FCT_ORDER` | MV | `dlt_datamart` pipeline | `DWH_ORDER_HEADER WHERE WA_ISCURR=1` |
| `datamart.FCT_SALES_LINE` | MV (Liquid Clustering) | `dlt_datamart` pipeline | `DWH_ORDER_HEADER ⨝ DWH_ORDER_DETAIL` (half-open SCD2-interval) |

**Folders:**
- `views/integration/sales_line.ipynb` — integration view-notebook (zie §7)
- `views/datamart/dim_*.ipynb` — 9 datamart dim-notebooks
- `views/07_apply_views.ipynb` — orchestrator die de 10 view-notebooks aanroept via `dbutils.notebook.run()`
- `datamart/*.sql` — twee MV's (`FCT_ORDER`, `FCT_SALES_LINE`), draaien in DLT-pipeline `dlt_datamart`

### Sleutelkeuze: wanneer view, wanneer MV? (ADR-0020)

| Vraag | View | MV |
|---|---|---|
| Heeft het object state nodig? (CDF-cursor, MERGE-progressie) | ❌ → Streaming Table | ❌ → Streaming Table |
| Is herberekening op elke query goedkoop? (lage cardinaliteit, simpele projectie) | ✅ | — |
| Voegt materialisatie aantoonbaar waarde toe? (Liquid Clustering, zware joins/aggregaten) | — | ✅ |

Voor de demo: dims zijn projecties met latest-row-per-BK over `DWH_` — goedkoop genoeg om altijd te herberekenen, en als view direct queryable door dashboards en AI/BI Genie. De feiten zijn MV's: `FCT_ORDER` is order-grain en propageert automatisch wanneer `DW_ORDER_HEADER` wijzigt; `FCT_SALES_LINE` is regel-grain met een SCD2-join — daar verdient Liquid Clustering de storage-kost terug.

### Sleutelstrategie — IDENTITY-surrogates + `yyyymmdd` voor `DIM_DATE`

KRM's surrogaat-model is gebaseerd op `BIGINT IDENTITY` (ADR-0010): `WK_<TABEL>` is een versie-niveau surrogaat in `DW_<TABEL>`, `WKR_<TABEL>` is de root (eerste versie ooit voor die BK) via FIRST_VALUE in `DWH_<TABEL>`. De datamart-laag promoveert deze surrogates één-op-één naar `MK_<NAAM>`:

- **SCD1 dim**: `MK_<NAAM> = WKR_<TABEL>` (root — stabiel over alle updates/deletes van de entiteit)
- **SCD2 dim**: `MK_<NAAM> = WK_<TABEL>` (versie-niveau); `MK_ROOT = WKR_<TABEL>`
- **`DIM_DATE`**: `MK_DATE = CAST(date_format(full_date, 'yyyyMMdd') AS INT)` — geen surrogaat, datum is intrinsiek deterministisch (ADR-0018)

Geen SHA2-hashes voor sleutels. SHA2-256 wordt enkel gebruikt voor `WA_HASH` in de tagged source MV (ADR-0019).

### `__UNKNOWN__`-orphans

NULL-natuurlijke-sleutels (warn-regels: `truck_id`, `location_id`, `shift_id`, `discount_id`) verschijnen als orphan-rijen in `DWH_<TABEL>` met BK=NULL. De SCD1 latest-row-per-BK selectie behandelt deze als één rij per "BK=NULL"-bucket; de fact-zijde joint via de NULL-BK direct en facts met ontbrekende attributen blijven dus zichtbaar in BI. Geen SHA2-collapse meer nodig — de orphan-semantiek leeft in de view-projectie.

### Tabelspec-conventie

Per dim/fact wordt eerst een **header-tabel** getoond met objectnamen en hun bron, daarna een **kolom-tabel** met types en herkomst. Deze vorm is uniform over §7 en §8.

### Tabelspec — `DIM_DATE` (ADR-0018)

| Object | Type | Bron |
|---|---|---|
| `datamart.DIM_DATE` | View | `SEQUENCE(MIN(order_ts), MAX(order_ts))` uit `DWH_ORDER_HEADER WHERE WA_ISCURR=1` |

| Kolom | Type | Herkomst |
|---|---|---|
| `MK_DATE` | INT | `CAST(date_format(full_date, 'yyyyMMdd') AS INT)` |
| `full_date` | DATE | exploded date |
| `year`, `quarter`, `month`, `day` | INT | `EXTRACT(...)` op `full_date` |
| `month_name`, `day_name` | STRING | `date_format(full_date, ...)` |
| `day_of_week`, `week_of_year` | INT | `EXTRACT(...)` |
| `is_weekend` | BOOLEAN | `day_of_week IN (6,7)` |
| `year_month_start`, `year_quarter_start`, `year_start` | DATE | `DATE_TRUNC(...)` |

Multipele datum-rollen op een fact (`order_ts`, `served_ts`, …) joinen via FK-naam (`MK_ORDERDATE`, `MK_SERVEDDATE`) op dezelfde `DIM_DATE` (ADR-0018).

### Tabelspec — SCD1 entity-dims (ADR-0012)

Header-zijde dims (`DIM_TRUCK`, `DIM_LOCATION`, `DIM_CUSTOMER`, `DIM_CURRENCY`, `DIM_ORDER_CHANNEL`, `DIM_SHIFT`, `DIM_DISCOUNT`) en detail-zijde `DIM_MENU_ITEM` volgen één SCD1-patroon.

| Object | Type | Bron |
|---|---|---|
| `datamart.DIM_<NAAM>` | View | `DWH_<TABEL>` — latest-row-per-BK by `WA_FROMDATE` (NOT `WA_ISCURR=1`, zodat ge-deleted entiteiten zichtbaar blijven) |

| Kolom | Type | Berekening |
|---|---|---|
| `MK_<NAAM>` | BIGINT | `WKR_<TABEL>` van de latest-row (root surrogaat — stabiel) |
| `<natural_key>` | bron-type | passthrough vanuit `DWH_` |
| beschrijvende attributen | bron-type | passthrough vanuit `DWH_` (latest-row-versie) |
| `MA_CREATEDATE` | TIMESTAMP | `MIN(WA_CRUDDTS) PARTITION BY <BK>` (moment van eerste C-event) |
| `MA_CHANGEDATE` | TIMESTAMP | `MAX(WA_CRUDDTS) FILTER (WHERE WA_CRUD <> 'C') PARTITION BY <BK>` — updates **én** deletes; NULL als entiteit alleen zijn initiële C-rij heeft |
| `MA_ISDEL` | INT | `CASE WHEN latest-row.WA_UNTODATE <> TIMESTAMP '9999-12-31 00:00:00' THEN 1 ELSE 0 END` |

Onder ADR-0010's `FLOW AUTO CDC STORED AS SCD TYPE 2` mechanisme heeft een ge-deleted entiteit géén `WA_ISCURR=1`-rij; daarom selecteert de view de latest row by `WA_FROMDATE`, niet by `WA_ISCURR`. De `MA_CHANGEDATE`-filter `WA_CRUD <> 'C'` zorgt dat het delete-moment de "last changed"-tijd wordt.

### Tabelspec — SCD2 entity-dims (ADR-0013)

Voor entiteiten waar de demo historische versies blootstelt aan consumers (bijv. een `DIM_<NAAM>_HISTORY` variant — niet actief in deze demo, maar het patroon staat hier voor consistentie):

| Object | Type | Bron |
|---|---|---|
| `datamart.DIM_<NAAM>` | View | `DWH_<TABEL>` — alle versies, één rij per `DWH_`-rij |

| Kolom | Type | Berekening |
|---|---|---|
| `MK_<NAAM>` | BIGINT | `WK_<TABEL>` (versie-niveau surrogaat) |
| `MK_ROOT` | BIGINT | `WKR_<TABEL>` — gedeeld over alle versies van één BK-lineage |
| `<natural_key>` | bron-type | passthrough |
| beschrijvende attributen | bron-type | passthrough — versie-specifiek |
| `MA_FROM` | TIMESTAMP | `WA_FROMDATE` |
| `MA_UNTO` | TIMESTAMP | `WA_UNTODATE` |
| `MA_ISCURR` | INT | `WA_ISCURR` |

Ge-deleted entiteiten hebben geen `MA_ISCURR=1`-rij; "is deze entiteit deleted?" wordt beantwoord door `latest rij per MK_ROOT heeft MA_UNTO <> '9999-12-31'` te checken (ADR-0013).

### Tabelspec — `DIM_SHIFT` (extra attributen)

| Kolom | Type | Berekening |
|---|---|---|
| `MK_SHIFT` | BIGINT | `WKR_ORDER_HEADER` van latest-row per `shift_id` |
| `shift_id` | DECIMAL(38,0) | natuurlijke sleutel |
| `shift_start_time`, `shift_end_time` | STRING `'HH:mm:ss'` | passthrough |
| `shift_duration_minutes` | INT | `(UNIX_TIMESTAMP(end,'HH:mm:ss') - UNIX_TIMESTAMP(start,'HH:mm:ss')) / 60` |
| `MA_CREATEDATE`, `MA_CHANGEDATE`, `MA_ISDEL` | … | per SCD1-patroon hierboven |

### Tabelspec — `FCT_ORDER` (ADR-0020)

Order-grain feitentabel. Eén rij per `order_id`. Leest direct uit `DWH_ORDER_HEADER WHERE WA_ISCURR=1` — SCD1-dims aan de header-zijde collapsen versies, dus de huidige rij volstaat.

| Object | Type | Bron |
|---|---|---|
| `datamart.FCT_ORDER` | MV | `DWH_ORDER_HEADER WHERE WA_ISCURR=1` |

| Kolom | Type | Categorie / Herkomst |
|---|---|---|
| `MK_DATE` | INT | `CAST(date_format(order_ts, 'yyyyMMdd') AS INT)` — join op `DIM_DATE` |
| `MK_TRUCK`, `MK_LOCATION`, `MK_CUSTOMER`, `MK_SHIFT`, `MK_CURRENCY`, `MK_ORDER_CHANNEL`, `MK_DISCOUNT` | BIGINT | `WKR_ORDER_HEADER` (root surrogaat — match met SCD1-dim's `MK_<NAAM>`) |
| `order_id` | DECIMAL(38,0) | degenerate dim |
| `order_ts`, `served_ts` | TIMESTAMP | event tijden |
| `order_amount`, `order_tax_amount`, `order_discount_amount`, `order_total` | DECIMAL(38,4) | measures |
| `time_to_serve_seconds` | BIGINT | derived: `served_ts - order_ts` |

### Tabelspec — `FCT_SALES_LINE` (ADR-0020)

Regel-grain feitentabel. Eén rij per `order_detail_id`. Leest `DWH_ORDER_HEADER ⨝ DWH_ORDER_DETAIL` direct via een half-open SCD2-interval-join: voor elke detail-event-tijd wordt de header-versie gepicked die op dat moment current was.

| Object | Type | Bron |
|---|---|---|
| `datamart.FCT_SALES_LINE` | MV (Liquid Clustering) | `DWH_ORDER_HEADER ⨝ DWH_ORDER_DETAIL ON order_id AND (header.WA_FROMDATE <= detail.order_ts < header.WA_UNTODATE)` |

| Kolom | Type | Categorie / Herkomst |
|---|---|---|
| `MK_DATE` | INT | `CAST(date_format(order_ts, 'yyyyMMdd') AS INT)` |
| `MK_TRUCK`, `MK_LOCATION`, `MK_CUSTOMER`, `MK_SHIFT`, `MK_CURRENCY`, `MK_ORDER_CHANNEL`, `MK_DISCOUNT` | BIGINT | `WKR_ORDER_HEADER` van de gepickte header-versie |
| `MK_MENU_ITEM` | BIGINT | `WKR_ORDER_DETAIL` van de detail-versie |
| `order_id`, `order_detail_id`, `line_number` | DECIMAL(38,0) | degenerate dims |
| `order_ts`, `served_ts` | TIMESTAMP | event tijden (gedenormaliseerd uit header) |
| `quantity`, `unit_price`, `price`, `order_item_discount_amount` | DECIMAL(38,4) | measures |
| `line_subtotal` | DECIMAL(38,4) | derived: `quantity * unit_price` |

**Liquid Clustering:** `CLUSTER BY (MK_TRUCK, MK_LOCATION, MK_DATE, MK_CURRENCY)` — dashboards en Genie filteren op verschillende combinaties van deze vier sleutels. Geen handmatige partition-keuze nodig; Databricks past clustering automatisch aan op queries.

### Join-semantiek per fact-dim-combinatie

| Fact-zijde | Dim-type | Join-mechanisme |
|---|---|---|
| `FCT_ORDER` met SCD1-dim | SCD1 (latest-row-per-BK) | `FCT_ORDER.MK_<NAAM> = DIM_<NAAM>.MK_<NAAM>` (beide `WKR_<TABEL>`) |
| `FCT_SALES_LINE` met SCD1-dim | SCD1 | idem — gepickte header-versie's `WKR_` matcht de latest-row-per-BK in de dim |
| Een toekomstige fact met SCD2-dim | SCD2 (alle versies) | half-open interval `FCT.WA_FROMDATE <= event_ts < FCT.WA_UNTODATE` — picks entity-versie current at event time |

### Workflow-integratie

```
setup
  ├─→ ingest_azurestorage_full
  └─→ ingest_azurestorage_incremental
       ↓
       dlt_integration              (DLT — tagged MVs + DW_* + DWH_* + DWQ_* per entiteit)
        ├─→ dlt_datamart            (DLT — FCT_ORDER MV + FCT_SALES_LINE MV met Liquid Clustering)
        └─→ apply_views             (notebook — SALES_LINE view + 9 DIM_* views)
```

Per ADR-0020 lezen `FCT_*` MV's direct uit `DWH_<TABEL>` (niet uit `DIM_<NAAM>`); `dlt_datamart` en `apply_views` mogen dus parallel na `dlt_integration` draaien.

1. **`dlt_integration`** bouwt de Streaming Tables en views per integration-entiteit.
2. **`apply_views`** (`views/07_apply_views.ipynb`) roept alle view-notebooks aan via `dbutils.notebook.run()` — `integration/sales_line` + de 9 `datamart/dim_*` notebooks. Geen storage, altijd vers.
3. **`dlt_datamart`** materialiseert beide feiten: `FCT_ORDER` (plain MV) en `FCT_SALES_LINE` (Liquid Clustering). Twee MV's — `datamart/fact_order.sql` + `datamart/fact_sales_line.sql`.

### Consumption-laag

**AI/BI Dashboard** — `dashboards/tasty_bytes_sales.lvdash.json` is gecheckt-in en wordt automatisch ge-deployed via `resources/dashboard.yml`. Alle widget-queries gebruiken `FCT_ORDER ⨝ DIM_*` joins:
- Revenue trend (line chart) — `FCT_ORDER ⨝ DIM_DATE ⨝ DIM_CURRENCY`
- Top trucks by revenue (bar) — `FCT_ORDER ⨝ DIM_TRUCK`
- Top locations by revenue (bar) — `FCT_ORDER ⨝ DIM_LOCATION`
- KPI cards — `SUM(order_total)` + `COUNT(*)` over `FCT_ORDER`

**AI/BI Genie space** — post-deploy handmatig geconfigureerd (Genie spaces serializen nog niet schoon in DAB). De runbook-stap staat in `docs/demo_script.md`:
- Maak een Genie space met `datamart.FCT_SALES_LINE` plus alle `datamart.DIM_*`-tabellen
- Voeg voorbeeldvragen toe ("Welke truck had vorige week de meeste revenue?", "Vergelijk revenue per uur van de dag tussen truck X en Y")

### Wat de datamart-laag specifiek demonstreert

- **Kimball star schema in pure SQL** — declaratieve `CREATE OR REPLACE VIEW` voor 9 dims + `CREATE OR REFRESH MATERIALIZED VIEW` voor 2 facts, geen Python in de datamart-laag
- **IDENTITY-surrogates over hashes** — `MK_<NAAM> = WKR_<TABEL>` stabiel over updates/deletes, gegenereerd door `FLOW AUTO CDC STORED AS SCD TYPE 2` (ADR-0010)
- **Latest-row-per-BK voor SCD1** — ge-deleted entiteiten blijven zichtbaar (ADR-0012)
- **`MK_DATE = yyyymmdd INT`** — leesbaar, deterministisch, lexicografisch sorteerbaar (ADR-0018)
- **Facts lezen `DWH_` direct** — geen tussentijdse dim-lookup, fact-build onafhankelijk van consumer dims (ADR-0020)
- **Half-open SCD2-interval-join** in `FCT_SALES_LINE` — picks header-versie current at line event time
- **Materialised Views met auto-refresh** — integration-correcties propageren bij elke pipeline-run
- **Liquid Clustering** op `FCT_SALES_LINE` — geen handmatige partition-keuze
- **AI/BI Dashboard via DAB** — versioned dashboard-definitie deploys via `databricks bundle deploy`
- **AI/BI Genie** — natural-language queries op de star (`FCT_SALES_LINE` + dims, post-deploy setup)

---

## 9. Mapstructuur

```
databricks-demo/
├── databricks.yml                      # DAB bundle root: name, includes, variables, targets (dev/test/prod)
├── resources/                          # DAB resource definitions (YAML)
│   ├── sqlserver.yml                   # Lakeflow Connect gateway + ingestion pipeline (geparkeerd)
│   ├── sqlserver_job.yml               # Geplande Job voor SQL Server pipeline (geparkeerd)
│   ├── dlt_integration.yml             # DLT pipeline definition — integration-laag (tagged MV + DW_ ST + DWH_ view + DWQ_ ST per entiteit)
│   ├── dlt_datamart.yml                # DLT pipeline definition — datamart-laag (FCT_ORDER MV + FCT_SALES_LINE MV met Liquid Clustering)
│   ├── dashboard.yml                   # DAB resource — deploys de AI/BI Dashboard
│   └── demo_workflow.yml               # End-to-end Workflow (setup → ingest_* → dlt_integration → (dlt_datamart || apply_views))
├── config/
│   └── 00_setup.ipynb                  # Catalog, schemas, volume én control table — alles in één (control table seed met STG_<TABEL> targets)
├── staging/
│   ├── 02_ingest_azurestorage.ipynb    # Parquet inladen via control table (mode=full|incremental|both); schrijft SA_CRUDDTS/SA_SRC/SA_RUNID
│   └── schema_inspector.ipynb          # Diagnostisch — bron-schema's inspecteren
├── integration/                        # DLT pipeline source folder — één .sql per entiteit (glob include)
│   ├── order_header.sql                # tagged MV order_header_src + DW_ORDER_HEADER ST + DWH_ORDER_HEADER view + DWQ_ORDER_HEADER ST
│   └── order_detail.sql                # idem voor order_detail
├── views/                              # Plain UC views (niet-DLT) — orchestrator + één notebook per view
│   ├── 07_apply_views.ipynb            # Orchestrator-task — roept met dbutils.notebook.run() elk view-notebook aan
│   ├── integration/
│   │   └── sales_line.ipynb            # SALES_LINE view: half-open SCD2-join DWH_ORDER_HEADER ⨝ DWH_ORDER_DETAIL
│   └── datamart/
│       ├── dim_date.ipynb              # DIM_DATE — SEQUENCE+EXPLODE met MK_DATE = yyyymmdd INT (ADR-0018)
│       ├── dim_truck.ipynb             # DIM_TRUCK — SCD1 latest-row-per-BK uit DWH_ORDER_HEADER (ADR-0012)
│       ├── dim_location.ipynb          # DIM_LOCATION — idem
│       ├── dim_customer.ipynb          # DIM_CUSTOMER — idem
│       ├── dim_menu_item.ipynb         # DIM_MENU_ITEM — SCD1 latest-row-per-BK uit DWH_ORDER_DETAIL
│       ├── dim_currency.ipynb          # DIM_CURRENCY — idem (header-zijde)
│       ├── dim_order_channel.ipynb     # DIM_ORDER_CHANNEL — idem
│       ├── dim_shift.ipynb             # DIM_SHIFT — idem + duration_minutes derived
│       └── dim_discount.ipynb          # DIM_DISCOUNT — idem
├── datamart/                           # DLT pipeline source folder — 2 .sql files (glob include)
│   ├── fact_order.sql                  # FCT_ORDER MV — leest DWH_ORDER_HEADER WHERE WA_ISCURR=1 (ADR-0020)
│   └── fact_sales_line.sql             # FCT_SALES_LINE MV (Liquid Clustering) — half-open SCD2-join DWH_ORDER_HEADER ⨝ DWH_ORDER_DETAIL
├── dashboards/
│   └── tasty_bytes_sales.lvdash.json   # AI/BI Dashboard definitie (auto-deploys via DAB) — leest FCT_ORDER + DIM_*
├── demo_showcase/
│   ├── delta_time_travel.ipynb         # Delta Time Travel demo
│   ├── audit_logs.ipynb                # Audit Logs demo
│   └── lineage.ipynb                   # Lineage demo
└── docs/
    ├── adr/                            # 20 ADRs — Accepted, target state (0003–0020 binding)
    ├── prerequisites.md                # Layer 1 admin setup (Access Connector, Storage Credential, etc.)
    └── demo_script.md                  # Handmatig demo-draaiboek met talking points en SQL snippets
```

### KRM-laag-naamgeving en object-conventies

| Laag (KRM) | Schema | Objectprefixen | Admin-kolommen |
|---|---|---|---|
| Ingest (Staging) | `staging_<bron>` | `STG_<TABEL>` | `SA_CRUDDTS`, `SA_SRC`, `SA_RUNID` (alleen Auto Loader; ADR-0017) |
| Combine (Integration) | `integration` | `DW_<TABEL>`, `DWH_<TABEL>`, `DWQ_<TABEL>` | `WA_CRUDDTS`, `WA_SRC`, `WA_RUNID`, `WA_CRUD`, `WA_HASH`, `WA_FROMDATE`, `WA_UNTODATE`, `WA_ISCURR` |
| Publish (Datamart) | `datamart` | `DIM_<NAAM>`, `FCT_<NAAM>` | `MK_<NAAM>`, `MK_ROOT` (SCD2), `MA_CREATEDATE`, `MA_CHANGEDATE`, `MA_ISDEL`, `MA_FROM`, `MA_UNTO`, `MA_ISCURR` |

Surrogates: `WK_<TABEL>` (BIGINT IDENTITY, versie-niveau, in `DW_<TABEL>`), `WKP_<TABEL>` (BIGINT, vorige versie, in `DWH_<TABEL>` view), `WKR_<TABEL>` (BIGINT, root — eerste versie ooit, in `DWH_<TABEL>` view). Datamart-zijde: `MK_<NAAM> = WKR_<TABEL>` voor SCD1 (ADR-0012), `MK_<NAAM> = WK_<TABEL>` voor SCD2 (ADR-0013), `MK_DATE = yyyymmdd INT` voor `DIM_DATE` (ADR-0018).

> **Quarantine — single file per entiteit (ADR-0011):** de oude gepaarde `_quarantine.sql`-bestanden zijn weg. Tagged source MV + `DW_<TABEL>` + `DWH_<TABEL>` + `DWQ_<TABEL>` leven samen in één `integration/<entiteit>.sql`.

> **Geen `03_ingest_sqlserver.ipynb`:** SQL Server wordt volledig declaratief ingeladen via Lakeflow Connect (`resources/sqlserver.yml`). LC-tabellen dragen géén `SA_*`-kolommen (ADR-0017) — de integration-laag adapteert per bron. Geen `01_create_volumes.ipynb`; volume-creatie zit in het setup-notebook.

> **Notebook-formaat:** Databricks gebruikt sinds de recente platform-update standaard `.ipynb` (Jupyter-formaat) voor nieuwe notebooks in plaats van `.py` (source-formaat). Alle nieuwe notebooks in deze demo zijn dus `.ipynb`.

---

## 10. Notebook Structuur (standaard per notebook)

Elke notebook volgt deze vaste opbouw:

| Cel | Type | Inhoud |
|---|---|---|
| 1 | `%md` | Titel & beschrijving |
| 2 | Python | Parameters & widgets (omgeving-selectie) |
| 3 | Python | Control table inlezen |
| 4 | Python | Logica (inladen, schrijven) |
| 5 | Python | Validatie & row counts |
| 6 | `%md` / Python | Resultaat tonen |

---

## 11. Demo-secties

### 11.1 Delta Time Travel
Tonen hoe je teruggaat naar een vorige versie van de control table:

```sql
-- Huidige versie
SELECT * FROM DEMO.CONFIG.pipeline_sources

-- Vorige versie
SELECT * FROM DEMO.CONFIG.pipeline_sources VERSION AS OF 1

-- Op tijdstip
SELECT * FROM DEMO.CONFIG.pipeline_sources TIMESTAMP AS OF '2024-01-01'
```

### 11.2 Unity Catalog Audit Logs
Tonen wie wat heeft gewijzigd in de control table en pipelines.

### 11.3 Lineage
Visueel tonen welke pipeline welke tabel heeft aangeraakt via de Unity Catalog Lineage viewer.

### 11.4 Metadata-gedreven switchen
Live voor een klant demonstreren dat één UPDATE in de control table het gedrag van de pipeline verandert — zonder code aan te passen.

---

## 12. Implementatievolgorde

1. **Layer 1 prerequisites** (eenmalig, handmatig) — Access Connector, Storage Credential, External Location, UC Connection voor SQL Server. Zie `docs/prerequisites.md`.
2. Azure DevOps repo koppelen aan Databricks Repos
3. `databricks.yml` opzetten met variabelen en drie targets (alleen `dev` echt gevuld)
4. `config/00_setup.ipynb` — catalog, schemas, volume én control table aanmaken (idempotent)
5. `staging/02_ingest_azurestorage.ipynb` — parquet inladen via control table (`mode=full|incremental|both`, `reset` widget)
6. `resources/demo_workflow.yml` — Workflow met `setup → (ingest_full || ingest_incremental)`
7. *(Geparkeerd)* `resources/sqlserver.yml` + `resources/sqlserver_job.yml` — Lakeflow Connect op `staging_sqlserver`
8. **Integration-laag (DLT)** — `integration/*.sql` (één bestand per entiteit, glob include in `resources/dlt_integration.yml`). Per entiteit: tagged source MV + `DW_<TABEL>` ST + `DWH_<TABEL>` view + `DWQ_<TABEL>` ST. `STREAM table_changes('staging_*.STG_<TABEL>', 1)` als bron, `FLOW AUTO CDC ... STORED AS SCD TYPE 2 SEQUENCE BY _commit_timestamp APPLY AS DELETE WHEN WA_CRUD='D'`. Per-rule `EXPECT (NOT array_contains(failed_rules, '<rule>'))` constraints + fail-grade `EXPECT (... ) ON VIOLATION FAIL UPDATE`. Type-fixes (string→decimal/timestamp, int-millis→`'HH:mm:ss'`).
9. **Views-laag** — één notebook per view: `views/integration/sales_line.ipynb` + `views/datamart/dim_*.ipynb` (9 dims). Elke notebook heeft één `%sql CREATE OR REPLACE VIEW` cel met `CREATE WIDGET TEXT catalog DEFAULT "DEMO"; USE CATALOG ${catalog}` ervoor. `views/07_apply_views.ipynb` is een Python-orchestrator die ze achter elkaar uitvoert via `dbutils.notebook.run()` en de `catalog`-widget doorgeeft. Geen storage, altijd vers tegen `DWH_*`. SCD1 latest-row-per-BK, `MK_<NAAM> = WKR_<TABEL>` (ADR-0012); `MK_DATE = yyyymmdd INT` voor `DIM_DATE` (ADR-0018).
10. **Datamart-laag (DLT)** — `datamart/*.sql` (2 feiten: `fact_order`, `fact_sales_line`) + `resources/dlt_datamart.yml` (glob include). `FCT_ORDER` is een plain MV op `DWH_ORDER_HEADER WHERE WA_ISCURR=1`; `FCT_SALES_LINE` heeft Liquid Clustering op de meest gefilterde FK-kolommen en leest `DWH_ORDER_HEADER ⨝ DWH_ORDER_DETAIL` direct via een half-open SCD2-interval-join (ADR-0020).
11. **AI/BI Dashboard** — `dashboards/tasty_bytes_sales.lvdash.json` + `resources/dashboard.yml`. Deploys via DAB met de pipeline.
12. `demo_showcase/` notebooks — Time Travel, Audit Logs, Lineage
13. `docs/demo_script.md` — handmatig demo-draaiboek schrijven (incl. Genie-space setup als post-deploy stap)

---

## 13. Belangrijke Adviezen

- **Gebruik Databricks Widgets** bovenaan elke notebook voor omgeving-selectie (`DEV`/`TEST`/`PROD`)
- **Schrijf elke notebook idempotent** — meerdere keren draaien geeft hetzelfde resultaat
- **Voeg row counts toe** na elke laadstap — klanten willen zien dat data aankomt
- **Gebruik `%md` cellen rijkelijk** — de notebook is ook het demo-document
- **Orchestratie loopt via een Databricks Workflow**, gedefinieerd in `resources/demo_workflow.yml`. `dbutils.notebook.run()` kan geen DLT- of Lakeflow Connect-pipelines triggeren; die hebben een Workflow met `pipeline_task` nodig. De Workflow is ook zelf een demo-moment (graph view, taakstatus, retries).
