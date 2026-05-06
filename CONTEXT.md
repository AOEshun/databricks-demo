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
Bronze-schema's zijn per bron (zodat laad-semantiek apart blijft); Silver en Gold zijn integraal — daar verdwijnt het bron-onderscheid bewust.

| Schema | Laag | Inhoud |
|---|---|---|
| `CONFIG` | Configuratie | Control table die de parquet-pipeline aanstuurt |
| `STAGING_AZURESTORAGE` | Staging / Bronze | Parquet-bronnen uit Azure Storage Container |
| `STAGING_SQLSERVER` | Staging / Bronze | SQL Server bron via Lakeflow Connect (geparkeerd) |
| `INTEGRATION` | Integration / Silver | Gecleande, gevalideerde, geïntegreerde tabellen + quarantine |
| `DATAMART` | Datamart / Gold | Materialised Views + AI/BI consumption-tabellen |

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
| `target_schema` | string | `STAGING_AZURESTORAGE` | Doelschema |
| `target_table` | string | `order_header` | Doeltabelnaam |
| `file_format` | string | `parquet` | Bestandstype |
| `is_active` | boolean | `true` | Aan/uit zonder rij te verwijderen |
| `load_type` | string | `full` | `full` of `incremental` (Auto Loader) |

> **Opmerking:** Alleen de parquet-pipeline leest deze control table. SQL Server wordt via Lakeflow Connect ingeladen en verschijnt niet in de control table.

### Initiële vulling

| source_system | source_path | file_pattern | target_table | load_type |
|---|---|---|---|---|
| `azurestorage` | `/Volumes/demo/staging_azurestorage/parquet` | `ORDER_HEADER*.parquet` | `order_header` | `full` |
| `azurestorage` | `/Volumes/demo/staging_azurestorage/parquet` | `ORDER_DETAIL*.parquet` | `order_detail` | `full` |

**Argumentatie:** De control table is een Delta-tabel en profiteert daarmee automatisch van Delta Time Travel, Unity Catalog Audit Logs en Lineage — zonder extra configuratie.

---

## 5. Audit-kolommen (per bron verschillend)

Audit-kolommen worden gesplitst per bron, omdat Lakeflow Connect zijn eigen doeltabellen beheert en geen ruimte biedt voor custom kolommen — terwijl het wel rijkere CDC-metadata levert dan we zelf zouden toevoegen.

### Azure Storage staging-tabellen (parquet)

Vijf custom kolommen worden toegevoegd aan elke parquet-doeltabel:

| Kolom | Inhoud |
|---|---|
| `_ingestion_timestamp` | `current_timestamp()` |
| `_source_system` | `azurestorage` |
| `_source_file` | Bestandspad uit `_metadata.file_path` |
| `_last_modified` | `_metadata.file_modification_time` van het bronbestand |
| `_pipeline_run_id` | Databricks job run id |

### SQL Server staging-tabellen (Lakeflow Connect)

Geen custom kolommen — Lakeflow Connect levert standaard zijn eigen CDC-metadata:

| Kolom | Inhoud |
|---|---|
| `_change_type` | `INSERT`, `UPDATE_PREIMAGE`, `UPDATE_POSTIMAGE`, `DELETE` |
| `_change_version` | Monotone CDC-versie van de bron |
| `_commit_timestamp` | Tijdstip van de wijziging in de bron |

> **Demo-waarde:** `_change_type` is een sterker demo-moment dan een eigen `_ingestion_timestamp` — klanten zien letterlijk welke rijen gewijzigd, ingevoegd of verwijderd zijn.

---

## 6. Pipeline Aanpak — Bronze (Staging)

### Eén notebook, drie modes

`staging/02_ingest_azurestorage.ipynb` is één notebook met een `mode` widget. De control table bepaalt **welke** tabel in welke mode hoort; het notebook doet alleen de mode die zijn widget zegt.

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
AND    target_table  = 'order_header'
```

Daarna verwerkt `mode=incremental` deze rij wél; `mode=full` slaat hem over.

### Reset widget — bij switch op een gevulde tabel

Wanneer een tabel die al via `full` is geladen wordt omgezet naar `incremental`, dupliceert de eerste incremental-run alle bestaande rijen: Auto Loader heeft geen checkpoint en behandelt elk bronbestand als nieuw.

Het notebook heeft daarom een `reset` widget (default `false`). Op `true`:
1. Drop de doeltabel
2. Verwijder de checkpoint-folder onder `_checkpoints/{target_table}/`
3. Auto Loader start schoon

De presentator zet `reset=true` expliciet bij de eerste incremental-run na een mode-switch — geen verborgen magie.

### Workflow

`resources/demo_workflow.yml` definieert één Workflow met drie taken:

```
setup
  ├─→ ingest_azurestorage_full         (mode=full,        parallel)
  └─→ ingest_azurestorage_incremental  (mode=incremental, parallel)
```

Beide ingest-taken wijzen naar **dezelfde** notebook met verschillende `mode`-parameters. Een taak waarvan de control-table-filter nul rijen oplevert is een no-op.

Voor demo-momenten waarin je gericht één mode wilt draaien: open het notebook in de Databricks UI, zet de `mode`-widget en klik Run.

### DLT verhuist naar de integration-laag

Staging laadt data zo dicht mogelijk bij de bron. Kwaliteitsregels en quarantine via DLT Expectations horen pas in de integration-laag — zie §7.

### Bronze-tabellen hebben Change Data Feed aan

Elke Bronze-tabel wordt aangemaakt met `delta.enableChangeDataFeed=true`. Dat maakt het mogelijk dat Silver via CDF leest en zowel inserts als overschrijvingen schoon verwerkt — zie §7 ("Bron-leespatroon").

---

## 7. Pipeline Aanpak — Silver (Integration)

### Doel van de laag

Silver levert de **Enterprise view**: gevalideerde, gestandaardiseerde en geïntegreerde data per bedrijfsentiteit. Type-fixes, snake_case-namen, kwaliteitscontroles en quarantine van slechte rijen horen hier — niet in Bronze.

### Eén DLT-pipeline, vijf tabellen in `INTEGRATION`

`integration/05_silver_dlt_pipeline.ipynb` definieert één Lakeflow Declarative Pipeline (DLT) met vijf tabellen:

| Tabel | Type | Inhoud |
|---|---|---|
| `INTEGRATION.order_header` | Streaming Table | Gecleaned `order_header` — rijen die alle drop-regels passeren |
| `INTEGRATION.order_header_quarantine` | Streaming Table | Rijen die één of meer drop-regels schenden + `failed_rules`-kolom |
| `INTEGRATION.order_detail` | Streaming Table | Gecleaned `order_detail` |
| `INTEGRATION.order_detail_quarantine` | Streaming Table | Schendende detail-rijen + `failed_rules`-kolom |
| `INTEGRATION.sales_line` | Materialised View | Geïntegreerde view: `order_header ⨝ order_detail` op `order_id`, één rij per orderregel |

**Naamgeving:** kolomnamen worden snake_case + Engels (`ORDER_ID` → `order_id`, `SHIFT_START_TIME` → `shift_start_time`, etc.). Silver is direct bruikbaar voor analisten zonder bron-jargon.

### Bron-leespatroon: Change Data Feed + apply_changes

Bronze schrijft via twee modes (`full` overschrijft, `incremental` appendt). Een gewone streaming-read op een tabel die overschreven wordt, faalt. Daarom:

1. Bronze-tabellen hebben `delta.enableChangeDataFeed=true` (zie §6).
2. Silver leest van de Bronze CDF: `spark.readStream.option("readChangeFeed", "true").table(...)`.
3. Silver gebruikt DLT's `apply_changes` (a.k.a. `auto_cdc`) om de change-events declaratief te MERGEn naar de Silver-tabel.

Effect: Bronze-overschrijvingen verschijnen in CDF als `delete_row` + `insert_row` events. Silver verwerkt beide zonder breken. Mode-switches in Bronze hebben nul impact op Silver pipeline-state. Hetzelfde patroon werkt later voor Lakeflow Connect's CDC-feed van SQL Server — geen herontwerp nodig.

### Quarantine-patroon (gepaarde tabellen)

Voor elke gecleande tabel bestaat een gespiegelde `_quarantine` tabel. Routering gebeurt via een filter op een gezamenlijk predicate:

```python
DROP_RULES = {
    "order_ts_not_null":     "order_ts IS NOT NULL",
    "customer_id_not_null":  "customer_id IS NOT NULL",
    "order_total_positive":  "order_total >= 0",
    # ...
}
clean_predicate = " AND ".join(f"({r})" for r in DROP_RULES.values())

# Cleansed: rijen die alle drop-regels passeren
.filter(clean_predicate)

# Quarantine: het inverse + welke regels gefaald zijn
.filter(f"NOT ({clean_predicate})")
.withColumn("failed_rules", build_failed_rules_array(DROP_RULES))
```

Drop-regels gooien rijen niet weg — ze landen in `_quarantine` met een `failed_rules ARRAY<STRING>` kolom. Triage:

```sql
SELECT * FROM INTEGRATION.order_header_quarantine
WHERE  array_contains(failed_rules, 'order_total_positive');
```

### Drie ernstniveaus

| Niveau | DLT-decorator / mechanisme | Effect |
|---|---|---|
| `warn` | `@dlt.expect_all(rules)` | Rij blijft in cleansed; schending wordt geteld in DLT-events |
| `drop` | filter + `_quarantine`-tabel | Rij wordt geroute naar `_quarantine` met `failed_rules` |
| `fail` | `@dlt.expect_all_or_fail(rules)` | Pipeline halt op schending — voor invariants die nooit mogen falen |

### Regel-set per tabel

**`INTEGRATION.order_header`:**

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

**`INTEGRATION.order_detail`:**

| Regel | Niveau |
|---|---|
| `order_detail_id IS NOT NULL` | fail |
| `order_id IS NOT NULL` | drop |
| `menu_item_id IS NOT NULL` | drop |
| `quantity > 0` | drop |
| `unit_price >= 0` | drop |
| `price >= 0` | drop |
| `line_number > 0` | warn |

### Type-fixes (Bronze → Silver)

Spark/Delta heeft geen native time-of-day type — de Bronze int-millis wordt daarom een `'HH:mm:ss'`-string in Silver (meest leesbaar; geen verwarrende `1970-01-01` placeholder-datum).

| Bronze-kolom | Bronze-type | Silver-type | Notitie |
|---|---|---|---|
| `SERVED_TS` | `StringType` | `TimestampType` | Parse `yyyy-MM-dd HH:mm:ss` |
| `ORDER_TAX_AMOUNT` | `StringType` | `DecimalType(38, 4)` | |
| `ORDER_DISCOUNT_AMOUNT` | `StringType` | `DecimalType(38, 4)` | |
| `ORDER_ITEM_DISCOUNT_AMOUNT` | `StringType` | `DecimalType(38, 4)` | |
| `LOCATION_ID` | `DoubleType` | `DecimalType(38, 0)` | IDs zijn geen Doubles |
| `DISCOUNT_ID` | `StringType` | `DecimalType(38, 0)` (nullable) | indien numeriek in bron |
| `SHIFT_START_TIME` | `IntegerType` (millis) | `StringType` `'HH:mm:ss'` | Bronze blijft de audit-trail van de raw int-millis |
| `SHIFT_END_TIME` | `IntegerType` (millis) | `StringType` `'HH:mm:ss'` | Idem |

### Workflow-integratie

```
setup
  ├─→ ingest_azurestorage_full         (mode=full)
  └─→ ingest_azurestorage_incremental  (mode=incremental)
       ↓
       dlt_integration                  (DLT pipeline_task)
```

De `dlt_integration` taak hangt af van **beide** ingest-taken. De DLT-pipeline zelf is gedefinieerd in `resources/dlt_integration.yml`.

### Wat Silver specifiek demonstreert

- **DLT Expectations** op drie ernstniveaus + tastbare quarantine-tabellen
- **Apply changes** voor change-data-capture handling vanuit Bronze CDF
- **Declaratieve graph view** in de DLT-UI met vijf nodes en hun afhankelijkheden
- **Materialised view** voor de geïntegreerde `sales_line`

---

## 8. Pipeline Aanpak — Gold (Datamart)

### Doel van de laag

Gold levert **consumption-ready, project-specifieke** data: gedenormaliseerde, lees-geoptimaliseerde tabellen waarop dashboards en AI/BI Genie direct kunnen draaien zonder joins. Per Databricks: "de-normalized and read-optimized data models with fewer joins" + Materialised Views voor frequent gequeryde metrics.

### Eén DLT-pipeline, vier tabellen in `DATAMART`

`datamart/06_gold_dlt_pipeline.ipynb` definieert één Lakeflow Declarative Pipeline met vier Materialised Views:

| Tabel | Grain | Bron | Doel |
|---|---|---|---|
| `DATAMART.daily_sales_by_truck` | (order_date, truck_id) | `INTEGRATION.order_header` | KPI: revenue per truck per dag |
| `DATAMART.daily_sales_by_location` | (order_date, location_id) | `INTEGRATION.order_header` | KPI: revenue per locatie per dag |
| `DATAMART.monthly_revenue_by_currency` | (year_month, order_currency) | `INTEGRATION.order_header` | Maandtrend per valuta |
| `DATAMART.sales_lines_wide` | per sales-line | `INTEGRATION.sales_line` | AI/BI Genie + Dashboards target |

**Bron-keuze:** aggregaten lezen van `order_header` (order-grain) om `SUM`-over-duplicated-line-rijen te voorkomen. Alleen `sales_lines_wide` leest van `sales_line` (line-grain).

### Tabelspec — daily aggregaten (truck en location)

| Kolom | Type | Berekening |
|---|---|---|
| `order_date` | DATE | `CAST(order_ts AS DATE)` |
| `truck_id` / `location_id` | DECIMAL(38, 0) | group key |
| `total_orders` | BIGINT | `COUNT(*)` |
| `total_revenue` | DECIMAL(38, 4) | `SUM(order_total)` |
| `total_tax` | DECIMAL(38, 4) | `SUM(order_tax_amount)` |
| `total_discount` | DECIMAL(38, 4) | `SUM(order_discount_amount)` |
| `avg_order_value` | DECIMAL(38, 4) | `total_revenue / total_orders` |

NULL `truck_id` / `location_id` rijen worden bewaard (`warn` in Silver, niet gequarantineerd) — ze verschijnen als één "Unknown"-rij in de aggregate. Dit maakt data-attributie-issues zichtbaar voor analisten.

### Tabelspec — `monthly_revenue_by_currency`

| Kolom | Type | Berekening |
|---|---|---|
| `year_month` | DATE | `DATE_TRUNC('month', order_ts)` (eerste van de maand) |
| `order_currency` | STRING | group key |
| `total_orders` | BIGINT | `COUNT(*)` |
| `total_revenue` | DECIMAL(38, 4) | `SUM(order_total)` |
| `avg_order_value` | DECIMAL(38, 4) | derived |

`year_month` is een echte DATE (eerste-van-de-maand), niet een string `'yyyy-MM'` — Databricks date-functions en BI-tools werken beter met DATE.

### Tabelspec — `sales_lines_wide`

Alle kolommen uit `INTEGRATION.sales_line` plus deze afgeleide kolommen voor AI/BI consumption:

| Afgeleide kolom | Type | Berekening |
|---|---|---|
| `order_date` | DATE | `CAST(order_ts AS DATE)` |
| `order_hour` | INT | `HOUR(order_ts)` (0-23) |
| `order_day_of_week` | STRING | `DATE_FORMAT(order_ts, 'EEEE')` ('Monday' etc.) |
| `order_year_month` | DATE | first-of-month |
| `shift_duration_minutes` | INT | `(parse(end) - parse(start)) / 60` |
| `line_subtotal` | DECIMAL(38, 4) | `quantity * unit_price` (sanity-check vs `price`) |

**Liquid Clustering** staat aan op deze tabel (`CLUSTER BY (truck_id, location_id, order_date, order_currency)`) omdat dashboards en Genie verschillende combinaties van filterkolommen gebruiken — geen handmatige partition-keuze nodig.

### Workflow-integratie

```
setup
  ├─→ ingest_azurestorage_full
  └─→ ingest_azurestorage_incremental
       ↓
       dlt_integration
        ↓
        dlt_datamart                 (DLT pipeline_task)
```

`dlt_datamart` hangt af van `dlt_integration`. De DLT-pipeline zelf is gedefinieerd in `resources/dlt_datamart.yml`.

### Consumption-laag

**AI/BI Dashboard** — `dashboards/tasty_bytes_sales.lvdash.json` is gecheckt-in en wordt automatisch ge-deployed via `resources/dashboard.yml`. Widgets:
- Revenue trend (line chart) — uit `monthly_revenue_by_currency`
- Top trucks by revenue (bar) — uit `daily_sales_by_truck`
- Top locations by revenue (bar) — uit `daily_sales_by_location`
- KPI card: totaal revenue + totaal orders

**AI/BI Genie space** — post-deploy handmatig geconfigureerd (Genie spaces serializen nog niet schoon in DAB). De runbook-stap staat in `docs/demo_script.md`:
- Maak een Genie space met `DATAMART.sales_lines_wide` als enige tabel
- Voeg voorbeeldvragen toe ("Welke truck had vorige week de meeste revenue?", "Vergelijk revenue per uur van de dag tussen truck X en Y")

### Wat Gold specifiek demonstreert

- **Materialised Views met auto-refresh** — declaratief gedefinieerde Gold-aggregaten die incrementeel verversen wanneer Silver verandert
- **Liquid Clustering** op de wide-tabel — geen handmatige partition-keuze, Databricks past clustering automatisch aan op queries
- **AI/BI Dashboard via DAB** — versioned dashboard-definitie deploys via `databricks bundle deploy`
- **AI/BI Genie** — natural-language queries op `sales_lines_wide` (post-deploy setup)

---

## 9. Mapstructuur

```
databricks-demo/
├── databricks.yml                      # DAB bundle root: name, includes, variables, targets (dev/test/prod)
├── resources/                          # DAB resource definitions (YAML)
│   ├── sqlserver.yml                   # Lakeflow Connect gateway + ingestion pipeline (geparkeerd)
│   ├── sqlserver_job.yml               # Geplande Job voor SQL Server pipeline (geparkeerd)
│   ├── dlt_integration.yml             # DLT pipeline definition voor Silver (INTEGRATION schema)
│   ├── dlt_datamart.yml                # DLT pipeline definition voor Gold (DATAMART schema)
│   ├── dashboard.yml                   # DAB resource — deploys de AI/BI Dashboard
│   └── demo_workflow.yml               # End-to-end Workflow (setup → ingest_* → dlt_integration → dlt_datamart)
├── config/
│   └── 00_setup.ipynb                  # Catalog, schemas, volume én control table — alles in één
├── staging/
│   ├── 02_ingest_azurestorage.ipynb    # Parquet inladen via control table (mode=full|incremental|both)
│   └── schema_inspector.ipynb          # Diagnostisch — bron-schema's inspecteren
├── integration/
│   └── 05_silver_dlt_pipeline.ipynb    # DLT pipeline: 5 tabellen in INTEGRATION + quarantine
├── datamart/
│   └── 06_gold_dlt_pipeline.ipynb      # DLT pipeline: 4 Materialised Views in DATAMART
├── dashboards/
│   └── tasty_bytes_sales.lvdash.json   # AI/BI Dashboard definitie (auto-deploys via DAB)
├── demo_showcase/
│   ├── delta_time_travel.ipynb         # Delta Time Travel demo
│   ├── audit_logs.ipynb                # Audit Logs demo
│   └── lineage.ipynb                   # Lineage demo
└── docs/
    ├── prerequisites.md                # Layer 1 admin setup (Access Connector, Storage Credential, etc.)
    └── demo_script.md                  # Handmatig demo-draaiboek met talking points en SQL snippets
```

> **Opmerking:** Er is geen `03_ingest_sqlserver.ipynb` notebook. SQL Server wordt volledig declaratief ingeladen via Lakeflow Connect (`resources/sqlserver.yml`). Er is ook geen `01_create_volumes.ipynb`; volume-creatie zit in het setup-notebook.

> **Notebook-formaat:** Databricks gebruikt sinds de recente platform-update standaard `.ipynb` (Jupyter-formaat) voor nieuwe notebooks in plaats van `.py` (source-formaat). Alle nieuwe notebooks in deze demo worden dus als `.ipynb` aangemaakt.

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
7. *(Geparkeerd)* `resources/sqlserver.yml` + `resources/sqlserver_job.yml` — Lakeflow Connect op `DEMO.STAGING_SQLSERVER`
8. **Integration-laag** — `integration/05_silver_dlt_pipeline.ipynb` + `resources/dlt_integration.yml`. DLT pipeline met Expectations (warn/drop/fail), gepaarde `_quarantine` tabellen, type-fixes (string→decimal/timestamp, int-millis→`'HH:mm:ss'`), `apply_changes` vanuit Bronze CDF, geïntegreerde `sales_line` MV.
9. **Datamart-laag** — `datamart/06_gold_dlt_pipeline.ipynb` + `resources/dlt_datamart.yml`. Vier Materialised Views in `DATAMART`: drie aggregaten (truck, location, currency-month) + één wide-tabel (`sales_lines_wide`) met Liquid Clustering voor AI/BI consumption.
10. **AI/BI Dashboard** — `dashboards/tasty_bytes_sales.lvdash.json` + `resources/dashboard.yml`. Deploys via DAB met de pipeline.
11. `demo_showcase/` notebooks — Time Travel, Audit Logs, Lineage
12. `docs/demo_script.md` — handmatig demo-draaiboek schrijven (incl. Genie-space setup als post-deploy stap)

---

## 13. Belangrijke Adviezen

- **Gebruik Databricks Widgets** bovenaan elke notebook voor omgeving-selectie (`DEV`/`TEST`/`PROD`)
- **Schrijf elke notebook idempotent** — meerdere keren draaien geeft hetzelfde resultaat
- **Voeg row counts toe** na elke laadstap — klanten willen zien dat data aankomt
- **Gebruik `%md` cellen rijkelijk** — de notebook is ook het demo-document
- **Orchestratie loopt via een Databricks Workflow**, gedefinieerd in `resources/demo_workflow.yml`. `dbutils.notebook.run()` kan geen DLT- of Lakeflow Connect-pipelines triggeren; die hebben een Workflow met `pipeline_task` nodig. De Workflow is ook zelf een demo-moment (graph view, taakstatus, retries).
