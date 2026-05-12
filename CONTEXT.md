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

### Eén DLT-pipeline + één view, vijf objecten in `INTEGRATION`

De `integration/`-folder bevat vier SQL DLT-bestanden (`integration/*.sql`), opgepikt via `libraries: - glob: include: integration/*.sql` in `resources/dlt_integration.yml`. Daarnaast bestaat `INTEGRATION.sales_line` als standaard SQL view (niet binnen DLT — zie `views/integration/sales_line.sql`, toegepast door `views/07_apply_views.ipynb`).

| Object | Type | Beheerd door | Inhoud |
|---|---|---|---|
| `INTEGRATION.order_header` | Streaming Table | DLT pipeline | Gecleaned `order_header` — rijen die alle drop-regels passeren |
| `INTEGRATION.order_header_quarantine` | Streaming Table | DLT pipeline | Schendende rijen + `failed_rules`-kolom |
| `INTEGRATION.order_detail` | Streaming Table | DLT pipeline | Gecleaned `order_detail` |
| `INTEGRATION.order_detail_quarantine` | Streaming Table | DLT pipeline | Schendende detail-rijen + `failed_rules`-kolom |
| `INTEGRATION.sales_line` | **View** | `apply_views` notebook | Pure join `order_header ⨝ order_detail` — altijd vers, geen storage |

**Naamgeving:** kolomnamen worden snake_case + Engels (`ORDER_ID` → `order_id`, etc.). Silver is direct bruikbaar voor analisten zonder bron-jargon.

### Bron-leespatroon: APPLY CHANGES FROM SNAPSHOT

Bronze schrijft via twee modes (`full` overschrijft, `incremental` appendt). Een gewone streaming-read op een overschreven tabel faalt of dupliceert. Daarom gebruikt Silver het **snapshot-diff**-patroon van DLT in pure SQL:

```sql
APPLY CHANGES INTO order_header
FROM SNAPSHOT order_header_clean_src
KEYS (order_id)
STORED AS SCD TYPE 1;
```

Per pipeline-run vergelijkt DLT de huidige snapshot van de bron met de vorige en MERGEt het verschil naar de target Streaming Table. Effect:
- Full overschrijvingen in Bronze: ontbrekende `order_id`s worden gedelete, nieuwe geïnsert, gewijzigde geüpdate.
- Incremental appends: alleen nieuwe rijen verschijnen als insert.
- Eén patroon dekt beide modes — geen CDF-metadata nodig, geen `_change_type`-handling in de Silver-code.

De type-fixes en drop-rule-filter zitten in een intermediaire Materialised View (`order_header_clean_src`) — die wordt elk pipeline-run volledig herberekend en is de snapshot-bron voor APPLY CHANGES.

### Quarantine-patroon (gepaarde bestanden)

Elke gecleande Streaming Table heeft een gespiegelde `_quarantine` Streaming Table in een apart SQL-bestand. Het quarantine-bestand gebruikt het **inverse filter** + bouwt een `failed_rules ARRAY<STRING>` op:

```sql
SELECT *,
  ARRAY_EXCEPT(
    ARRAY(
      CASE WHEN order_ts IS NULL       THEN 'order_ts_not_null'        END,
      CASE WHEN customer_id IS NULL    THEN 'customer_id_not_null'     END,
      CASE WHEN order_total < 0        THEN 'order_total_non_negative' END,
      ...
    ),
    ARRAY(CAST(NULL AS STRING))
  ) AS failed_rules
FROM typed
WHERE NOT (<clean predicate>);
```

Triage:

```sql
SELECT * FROM INTEGRATION.order_header_quarantine
WHERE  array_contains(failed_rules, 'order_total_non_negative');
```

### Drie ernstniveaus

| Niveau | Mechanisme in SQL DLT | Effect |
|---|---|---|
| `warn` | `CONSTRAINT ... EXPECT (...)` (zonder ON VIOLATION) | Rij blijft in cleansed; schending verschijnt in DLT-eventlog |
| `drop` | `WHERE`-filter in de clean source MV + inverse in quarantine | Rij gaat naar `_quarantine` met `failed_rules` |
| `fail` | `CONSTRAINT ... EXPECT (...) ON VIOLATION FAIL UPDATE` | Pipeline halt op schending |

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
       dlt_integration                  (DLT pipeline_task — 4 streaming tables)
        ↓
        apply_views                     (notebook task — sales_line view + datamart views)
```

De `dlt_integration` taak hangt af van **beide** ingest-taken. De `sales_line`-view wordt direct daarna door `apply_views` aangemaakt (zie §8).

### Wat Silver specifiek demonstreert

- **DLT Expectations** op drie ernstniveaus + tastbare quarantine-tabellen
- **APPLY CHANGES FROM SNAPSHOT** voor uniforme handling van full-overwrite én incremental-append modes — pure SQL, geen CDF-metadata in user code
- **Declaratieve graph view** in de DLT-UI met vier Streaming Tables + hun helper-MVs
- **Standaard SQL view** voor `sales_line` — geen materialisatie, altijd vers, demonstreert wanneer virtualisatie loont (zie §8 Sleutelkeuzes)

---

## 8. Pipeline Aanpak — Gold (Datamart, Star Schema)

### Doel van de laag

Gold levert **consumption-ready, project-specifieke** data in een klassieke Kimball **star schema** vorm: feiten- en dimensietabellen waarop dashboards en AI/BI Genie via simpele joins kunnen draaien. Per Databricks-best-practice: "de-normalized and read-optimized data models" + Materialised Views voor declaratieve, auto-refreshing aggregaten.

### Hybride architectuur — views voor virtualisatie, één MV voor performance

De Gold-laag is gesplitst over **twee beheermechanismen**, gekozen per object op basis van waar materialisatie waarde toevoegt:

| Tabel | Type | Beheerd door | Reden |
|---|---|---|---|
| `DATAMART.dim_date` | **View** | `apply_views` | Lage cardinaliteit, simpele projectie |
| `DATAMART.dim_truck` | **View** | `apply_views` | Idem |
| `DATAMART.dim_location` | **View** | `apply_views` | Idem |
| `DATAMART.dim_customer` | **View** | `apply_views` | Idem |
| `DATAMART.dim_menu_item` | **View** | `apply_views` | Idem |
| `DATAMART.dim_currency` | **View** | `apply_views` | Idem |
| `DATAMART.dim_order_channel` | **View** | `apply_views` | Idem |
| `DATAMART.dim_shift` | **View** | `apply_views` | Idem |
| `DATAMART.dim_discount` | **View** | `apply_views` | Idem |
| `DATAMART.fact_order` | **MV** | `dlt_datamart` pipeline | Order-grain fact, materialised zodat Silver-correcties automatisch propageren |
| `DATAMART.fact_sales_line` | **MV** (Liquid Clustering) | `dlt_datamart` pipeline | Zwaarste tabel, profiteert van clustering |

**Folders:**
- `views/integration/sales_line.sql` — Silver view (zie §7)
- `views/datamart/*.sql` — 9 Datamart dim-definities
- `datamart/*.sql` — twee MV's (`fact_order`, `fact_sales_line`), draaien in DLT-pipeline `dlt_datamart`

### Sleutelkeuze: wanneer view, wanneer MV?

| Vraag | View | MV |
|---|---|---|
| Heeft het object state nodig? (CDF-cursor, MERGE-progressie) | ❌ → Streaming Table | ❌ → Streaming Table |
| Is herberekening op elke query goedkoop? (lage cardinaliteit, simpele projectie) | ✅ | — |
| Voegt materialisatie aantoonbaar waarde toe? (Liquid Clustering, zware joins/aggregaten) | — | ✅ |

Voor de demo: alle dims voldoen aan "goedkoop genoeg om altijd te herberekenen" en blijven views. De feiten zijn MVs: `fact_order` is order-grain en zonder clustering al snel genoeg; `fact_sales_line` is regel-grain met 9 SHA2-berekeningen plus de upstream join — daar verdient Liquid Clustering de storage-kost terug.

### Sleutelstrategie — SHA2-surrogate keys

Elke dimensie heeft één surrogate key `dim_<entity>_key` van type `STRING` (64-char hex), gegenereerd via:

```sql
SHA2(COALESCE(CAST(<natural_key> AS STRING), '__UNKNOWN__'), 256)
```

De feiten berekenen exact dezelfde formule **inline** op hun eigen FK-kolommen — geen lookup-join nodig tijdens de load. NULL-natuurlijke-sleutels (warn-regels in Silver: `truck_id`, `location_id`, `shift_id`, `discount_id`) collapsen op één gedeelde *Unknown*-key per dim, zodat orders met ontbrekende attributen niet uit BI verdwijnen.

### Tabelspec — `dim_date`

Kalenderdimensie gegenereerd uit `[MIN(order_ts), MAX(order_ts)]` via `SEQUENCE` + `EXPLODE` — bevat ook dagen zonder orders zodat tijdseries geen gaten hebben.

| Kolom | Type |
|---|---|
| `dim_date_key` | STRING (SHA2) |
| `full_date` | DATE |
| `year`, `quarter`, `month`, `day` | INT |
| `month_name`, `day_name` | STRING |
| `day_of_week`, `week_of_year` | INT |
| `is_weekend` | BOOLEAN |
| `year_month_start`, `year_quarter_start`, `year_start` | DATE |

### Tabelspec — entity-dimensies

Alle entity-dims (`dim_truck`, `dim_location`, `dim_customer`, `dim_menu_item`, `dim_currency`, `dim_order_channel`, `dim_discount`) hebben dezelfde structuur:

| Kolom | Type |
|---|---|
| `dim_<entity>_key` | STRING (SHA2 surrogate) |
| `<natural_key>` | bron-type (DECIMAL of STRING) |

Rijkere beschrijvende attributen worden toegevoegd zodra de bron ze levert.

### Tabelspec — `dim_shift`

Extra beschrijvende attributen bovenop de natuurlijke sleutel:

| Kolom | Type | Berekening |
|---|---|---|
| `dim_shift_key` | STRING | SHA2 |
| `shift_id` | DECIMAL(38,0) | natuurlijke sleutel |
| `shift_start_time`, `shift_end_time` | STRING `'HH:mm:ss'` | passthrough |
| `shift_duration_minutes` | INT | `(UNIX_TIMESTAMP(end,'HH:mm:ss') - UNIX_TIMESTAMP(start,'HH:mm:ss')) / 60` |

### Tabelspec — `fact_order`

Order-grain feitentabel. Eén rij per `order_id`.

| Kolom | Type | Categorie |
|---|---|---|
| `dim_date_key`, `dim_truck_key`, `dim_location_key`, `dim_customer_key`, `dim_shift_key`, `dim_currency_key`, `dim_order_channel_key`, `dim_discount_key` | STRING | 8 FK's |
| `order_id` | DECIMAL(38,0) | degenerate dim |
| `order_ts`, `served_ts` | TIMESTAMP | event tijden |
| `order_amount`, `order_tax_amount`, `order_discount_amount`, `order_total` | DECIMAL(38,4) | measures |
| `time_to_serve_seconds` | BIGINT | derived measure: `served_ts - order_ts` |

### Tabelspec — `fact_sales_line`

Regel-grain feitentabel. Eén rij per `order_detail_id` in `INTEGRATION.sales_line`.

| Kolom | Type | Categorie |
|---|---|---|
| Alle 8 `fact_order`-FK's + `dim_menu_item_key` | STRING | 9 FK's |
| `order_id`, `order_detail_id`, `line_number` | DECIMAL(38,0) | degenerate dims |
| `order_ts`, `served_ts` | TIMESTAMP | event tijden (gedenormaliseerd) |
| `quantity`, `unit_price`, `price`, `order_item_discount_amount` | DECIMAL(38,4) | measures |
| `line_subtotal` | DECIMAL(38,4) | derived: `quantity * unit_price` |

**Liquid Clustering:** `CLUSTER BY (dim_truck_key, dim_location_key, dim_date_key, dim_currency_key)` — dashboards en Genie filteren op verschillende combinaties van deze vier sleutels. Geen handmatige partition-keuze nodig.

### Workflow-integratie

```
setup
  ├─→ ingest_azurestorage_full
  └─→ ingest_azurestorage_incremental
       ↓
       dlt_integration              (DLT — 4 Streaming Tables in INTEGRATION)
        ↓
        apply_views                 (notebook — Silver sales_line view + 9 Datamart dim views)
         ↓
         dlt_datamart               (DLT — fact_order MV + fact_sales_line MV met Liquid Clustering)
```

Drie volgordelijke stappen na ingest:
1. **`dlt_integration`** bouwt de Streaming Tables.
2. **`apply_views`** (`views/07_apply_views.ipynb`) past alle plain SQL views toe — `sales_line` eerst (Silver), daarna de 9 Datamart dim views. `fact_sales_line` MV in stap 3 leest van `sales_line` view, dus die moet hier al bestaan.
3. **`dlt_datamart`** materialiseert beide feiten: `fact_order` (order-grain, plain MV op `INTEGRATION.order_header`) en `fact_sales_line` (regel-grain, Liquid Clustering). Twee MV's in deze pipeline — `datamart/fact_order.sql` + `datamart/fact_sales_line.sql`.

### Consumption-laag

**AI/BI Dashboard** — `dashboards/tasty_bytes_sales.lvdash.json` is gecheckt-in en wordt automatisch ge-deployed via `resources/dashboard.yml`. Alle widget-queries gebruiken `fact_order ⨝ dim_*` joins:
- Revenue trend (line chart) — `fact_order ⨝ dim_date ⨝ dim_currency`
- Top trucks by revenue (bar) — `fact_order ⨝ dim_truck`
- Top locations by revenue (bar) — `fact_order ⨝ dim_location`
- KPI cards — `SUM(order_total)` + `COUNT(*)` over `fact_order`

**AI/BI Genie space** — post-deploy handmatig geconfigureerd (Genie spaces serializen nog niet schoon in DAB). De runbook-stap staat in `docs/demo_script.md`:
- Maak een Genie space met `DATAMART.fact_sales_line` plus alle `DATAMART.dim_*`-tabellen
- Voeg voorbeeldvragen toe ("Welke truck had vorige week de meeste revenue?", "Vergelijk revenue per uur van de dag tussen truck X en Y")

### Wat Gold specifiek demonstreert

- **Kimball star schema in pure SQL DLT** — declaratieve `CREATE OR REFRESH MATERIALIZED VIEW`-statements voor 9 dims + 2 facts, geen Python in de Gold-laag
- **SHA2-surrogate keys** — deterministisch, idempotent, NULL-safe via `COALESCE('__UNKNOWN__')`, geen IDENTITY-kolommen nodig
- **Unknown-leden** — orders met NULL truck/location/discount/shift blijven zichtbaar via één gedeelde Unknown-key per dim
- **Materialised Views met auto-refresh** — Silver-correcties propageren bij elke pipeline-run
- **Liquid Clustering** op de fact-tabel — geen handmatige partition-keuze, Databricks past clustering automatisch aan op queries
- **AI/BI Dashboard via DAB** — versioned dashboard-definitie deploys via `databricks bundle deploy`
- **AI/BI Genie** — natural-language queries op de star (`fact_sales_line` + dims, post-deploy setup)

---

## 9. Mapstructuur

```
databricks-demo/
├── databricks.yml                      # DAB bundle root: name, includes, variables, targets (dev/test/prod)
├── resources/                          # DAB resource definitions (YAML)
│   ├── sqlserver.yml                   # Lakeflow Connect gateway + ingestion pipeline (geparkeerd)
│   ├── sqlserver_job.yml               # Geplande Job voor SQL Server pipeline (geparkeerd)
│   ├── dlt_integration.yml             # DLT pipeline definition voor Silver (Streaming Tables)
│   ├── dlt_datamart.yml                # DLT pipeline definition voor Gold (fact_sales_line MV)
│   ├── dashboard.yml                   # DAB resource — deploys de AI/BI Dashboard
│   └── demo_workflow.yml               # End-to-end Workflow (setup → ingest_* → dlt_integration → apply_views → dlt_datamart)
├── config/
│   └── 00_setup.ipynb                  # Catalog, schemas, volume én control table — alles in één
├── staging/
│   ├── 02_ingest_azurestorage.ipynb    # Parquet inladen via control table (mode=full|incremental|both)
│   └── schema_inspector.ipynb          # Diagnostisch — bron-schema's inspecteren
├── integration/                        # DLT pipeline source folder — 4 .sql files (glob include)
│   ├── order_header.sql                # Cleansed Streaming Table + APPLY CHANGES FROM SNAPSHOT
│   ├── order_header_quarantine.sql     # Quarantine Streaming Table
│   ├── order_detail.sql
│   └── order_detail_quarantine.sql
├── views/                              # Plain SQL views (niet-DLT) + orchestrator
│   ├── 07_apply_views.ipynb            # Notebook task — itereert over integration/ + datamart/ en runt elk .sql bestand
│   ├── integration/
│   │   └── sales_line.sql              # Geïntegreerde header⨝detail view
│   └── datamart/
│       ├── dim_date.sql                # 9 dimensies …
│       ├── dim_truck.sql
│       ├── dim_location.sql
│       ├── dim_customer.sql
│       ├── dim_menu_item.sql
│       ├── dim_currency.sql
│       ├── dim_order_channel.sql
│       ├── dim_shift.sql
│       └── dim_discount.sql
├── datamart/                           # DLT pipeline source folder — 2 .sql files (glob include)
│   ├── fact_order.sql                  # Order-grain MV — leest INTEGRATION.order_header
│   └── fact_sales_line.sql             # Regel-grain MV (Liquid Clustering) — leest sales_line view
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
8. **Integration-laag (DLT)** — `integration/*.sql` (4 bestanden, glob include in `resources/dlt_integration.yml`). Streaming Tables met Expectations (warn/drop/fail), gepaarde `_quarantine` tabellen, type-fixes (string→decimal/timestamp, int-millis→`'HH:mm:ss'`), `APPLY CHANGES FROM SNAPSHOT` voor uniforme full/incremental-handling.
9. **Views-laag** — `views/integration/sales_line.sql` + `views/datamart/*.sql` (9 dim-bestanden). Toegepast door `views/07_apply_views.ipynb` als losse Workflow-task. Geen storage, altijd vers tegen Silver. SHA2-surrogate keys, NULL-safe via `COALESCE('__UNKNOWN__')`.
10. **Datamart-laag (DLT)** — `datamart/*.sql` (2 feiten: `fact_order`, `fact_sales_line`) + `resources/dlt_datamart.yml` (glob include). `fact_order` is een plain MV op `INTEGRATION.order_header`; `fact_sales_line` heeft Liquid Clustering op de meest gefilterde FK-kolommen en leest van `INTEGRATION.sales_line` view.
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
