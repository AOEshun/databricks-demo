# Databricks Demo Template — Architectuurbeslissingen

## Context

Dit document beschrijft alle architectuurbeslissingen voor een Databricks demo-omgeving.
De demo is bedoeld om de kracht en best practices van het Databricks-platform te tonen aan klanten.
De omgeving moet productie-waardig zijn in opzet en uitleg, ook al is het primair een demo.

---

## 1. Unity Catalog Structuur

### Catalogs
Drie omgevingen, elk als aparte catalog:

| Catalog | Omgeving |
|---|---|
| `DEMO_DEV` | Development |
| `DEMO_TEST` | Test |
| `DEMO_PROD` | Productie |

**Argumentatie:** Databricks best practice is omgevingsisolatie op catalog-niveau. Dit geeft de beste toegangscontrole via Unity Catalog en is herkenbaar voor klanten die dit zelf willen implementeren.

### Schema's (binnen elke catalog)
Schema's zijn gebaseerd op de laag én de bronbron:

| Schema | Laag | Bron |
|---|---|---|
| `CONFIG` | Configuratie | Bevat de control table die de parquet-pipeline aanstuurt |
| `STAGING_AZURESTORAGE` | Staging / Bronze | Azure Storage Container |
| `STAGING_SQLSERVER` | Staging / Bronze | Azure SQL Server |
| `INTEGRATION_...` | Integration / Silver | *(later uit te werken)* |
| `DATAMART_...` | Datamart / Gold | *(later uit te werken)* |

---

## 2. Bronnen

| Bron | Type | Inhoud |
|---|---|---|
| Azure Storage Container | Parquet-bestanden | `order_header`, `order_detail` |
| Azure SQL Server | SQL Database | 1 tabel *(naam nog onbekend)* |

---

## 3. Volumes

- **Één External Volume** op container-niveau, gekoppeld aan de Azure Storage Container
- De parquet-bestanden staan plat in één `source/` map, met bestandsnamen die met `ORDER_HEADER_` of `ORDER_DETAIL_` beginnen
- Routering naar de juiste doeltabel gebeurt via Auto Loader's `pathGlobFilter`, gestuurd door de `file_pattern` kolom in de control table

```
/Volumes/demo_dev/staging_azurestorage/source/
├── ORDER_HEADER_*.parquet
├── ORDER_DETAIL_*.parquet
└── _checkpoints/        # Auto Loader checkpoints per target table
```

---

## 4. Control Table

### Locatie
```
DEMO_DEV.CONFIG.pipeline_sources
```

### Kolommen

| Kolom | Type | Voorbeeld | Doel |
|---|---|---|---|
| `source_system` | string | `azurestorage` | Welk bronsysteem |
| `source_path` | string | `/Volumes/demo_dev/staging_azurestorage/source` | Pad naar de bronfolder |
| `file_pattern` | string | `ORDER_HEADER_*.parquet` | Glob filter binnen de folder (per doeltabel) |
| `target_schema` | string | `STAGING_AZURESTORAGE` | Doelschema |
| `target_table` | string | `order_header` | Doeltabelnaam |
| `file_format` | string | `parquet` | Bestandstype |
| `is_active` | boolean | `true` | Aan/uit zonder rij te verwijderen |
| `load_type` | string | `full` | `full` of `incremental` (Auto Loader) |

> **Opmerking:** Alleen de parquet-pipeline leest deze control table. SQL Server wordt via Lakeflow Connect ingeladen en verschijnt niet in de control table.

### Initiële vulling

| source_system | source_path | file_pattern | target_table | load_type |
|---|---|---|---|---|
| `azurestorage` | `/Volumes/demo_dev/staging_azurestorage/source` | `ORDER_HEADER_*.parquet` | `order_header` | `full` |
| `azurestorage` | `/Volumes/demo_dev/staging_azurestorage/source` | `ORDER_DETAIL_*.parquet` | `order_detail` | `full` |

**Argumentatie:** De control table is een Delta-tabel en profiteert daarmee automatisch van Delta Time Travel, Unity Catalog Audit Logs en Lineage — zonder extra configuratie.

---

## 5. Audit-kolommen (per bron verschillend)

Audit-kolommen worden gesplitst per bron, omdat Lakeflow Connect zijn eigen doeltabellen beheert en geen ruimte biedt voor custom kolommen — terwijl het wel rijkere CDC-metadata levert dan we zelf zouden toevoegen.

### Azure Storage staging-tabellen (parquet — basis-notebook én DLT)

Vijf custom kolommen worden toegevoegd aan elke parquet-doeltabel:

| Kolom | Inhoud |
|---|---|
| `_ingestion_timestamp` | `current_timestamp()` |
| `_source_system` | `azurestorage` |
| `_source_file` | Bestandspad uit `_metadata.file_path` |
| `_last_modified` | `_metadata.file_modification_time` van het bronbestand |
| `_pipeline_run_id` | Databricks job run id (basis) of `pipelines.id` (DLT) |

### SQL Server staging-tabellen (Lakeflow Connect)

Geen custom kolommen — Lakeflow Connect levert standaard zijn eigen CDC-metadata:

| Kolom | Inhoud |
|---|---|
| `_change_type` | `INSERT`, `UPDATE_PREIMAGE`, `UPDATE_POSTIMAGE`, `DELETE` |
| `_change_version` | Monotone CDC-versie van de bron |
| `_commit_timestamp` | Tijdstip van de wijziging in de bron |

> **Demo-waarde:** `_change_type` is een sterker demo-moment dan een eigen `_ingestion_timestamp` — klanten zien letterlijk welke rijen gewijzigd, ingevoegd of verwijderd zijn.

---

## 6. Pipeline Aanpak

### Laadstrategie
- **Standaard:** Full load
- **Demo-moment:** Live switchen naar incrementeel via één UPDATE in de control table:

```sql
UPDATE DEMO_DEV.CONFIG.pipeline_sources
SET load_type = 'incremental'
WHERE source_system = 'azurestorage'
AND target_table = 'order_header'
```

### Basis Pipeline vs DLT Pipeline
Beide pipelines schrijven naar **aparte tabellen** zodat ze naast elkaar gedemonstreerd kunnen worden:

| Tabel | Pipeline |
|---|---|
| `order_header` | Basis (notebook) |
| `order_detail` | Basis (notebook) |
| `order_header_dlt` | Delta Live Tables |
| `order_detail_dlt` | Delta Live Tables |
| `(sql tabel)` | Lakeflow Connect (geen DLT-variant) |

**Wat de DLT-pipeline specifiek demonstreert:** Expectations (declaratieve data-quality regels op drie ernstniveaus — `expect`, `expect_or_drop`, `expect_or_fail`) en de declaratieve graph view in de DLT-UI. Dit zijn features die het basis-notebook niet zonder significante custom code kan reproduceren. De DLT-pipeline leest **niet** uit de control table; de metadata-gedreven full↔incremental switch is exclusief een feature van de basis-pipeline. SQL Server kent geen DLT-variant omdat Lakeflow Connect al een complete declaratieve CDC-pipeline biedt.

---

## 7. Mapstructuur

```
databricks-demo/
├── databricks.yml                      # DAB bundle root: name, includes, variables, targets (dev/test/prod)
├── resources/                          # DAB resource definitions (YAML)
│   ├── sqlserver.yml                   # Lakeflow Connect gateway + ingestion pipeline
│   ├── sqlserver_job.yml               # Geplande Job (dagelijks 04:00 UTC) voor SQL Server pipeline
│   ├── dlt_staging.yml                 # DLT pipeline definition
│   └── demo_workflow.yml               # End-to-end Workflow (bootstrap → ingest → DLT → SQL Server)
├── config/
│   └── 00_bootstrap.py                 # Catalog, schemas, volume én control table — alles in één
├── staging/
│   ├── 02_ingest_azurestorage.py       # Parquet inladen via control table (Auto Loader)
│   └── dlt/
│       └── 04_staging_dlt_pipeline.py  # DLT versie van staging (Expectations + graph view)
├── integration/
│   └── (later uit te werken)
├── datamart/
│   └── (later uit te werken)
├── demo_showcase/
│   ├── delta_time_travel.py            # Delta Time Travel demo
│   ├── audit_logs.py                   # Audit Logs demo
│   └── lineage.py                      # Lineage demo
└── docs/
    ├── prerequisites.md                # Layer 1 admin setup (Access Connector, Storage Credential, etc.)
    └── demo_script.md                  # Handmatig demo-draaiboek met talking points en SQL snippets
```

> **Opmerking:** Er is geen `03_ingest_sqlserver.py` notebook. SQL Server wordt volledig declaratief ingeladen via Lakeflow Connect (`resources/sqlserver.yml`). Er is ook geen `01_create_volumes.py`; volume-creatie zit in het bootstrap-notebook.

---

## 8. Notebook Structuur (standaard per notebook)

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

## 9. Demo-secties

### 9.1 Delta Time Travel
Tonen hoe je teruggaat naar een vorige versie van de control table:

```sql
-- Huidige versie
SELECT * FROM DEMO_DEV.CONFIG.pipeline_sources

-- Vorige versie
SELECT * FROM DEMO_DEV.CONFIG.pipeline_sources VERSION AS OF 1

-- Op tijdstip
SELECT * FROM DEMO_DEV.CONFIG.pipeline_sources TIMESTAMP AS OF '2024-01-01'
```

### 9.2 Unity Catalog Audit Logs
Tonen wie wat heeft gewijzigd in de control table en pipelines.

### 9.3 Lineage
Visueel tonen welke pipeline welke tabel heeft aangeraakt via de Unity Catalog Lineage viewer.

### 9.4 Metadata-gedreven switchen
Live voor een klant demonstreren dat één UPDATE in de control table het gedrag van de pipeline verandert — zonder code aan te passen.

---

## 10. Implementatievolgorde

1. **Layer 1 prerequisites** (eenmalig, handmatig) — Access Connector, Storage Credential, External Location, UC Connection voor SQL Server. Zie `docs/prerequisites.md`.
2. Azure DevOps repo koppelen aan Databricks Repos
3. `databricks.yml` opzetten met variabelen en drie targets (alleen `dev` echt gevuld)
4. `config/00_bootstrap.py` — catalog, schemas, volume én control table aanmaken (idempotent)
5. `staging/02_ingest_azurestorage.py` — parquet inladen via Auto Loader, gestuurd door de control table
6. `resources/dlt_staging.yml` + `staging/dlt/04_staging_dlt_pipeline.py` — DLT pipeline met Expectations
7. `resources/sqlserver.yml` + `resources/sqlserver_job.yml` updaten — Lakeflow Connect richten op `DEMO_DEV.STAGING_SQLSERVER`, Job op dagelijks 04:00 UTC
8. `resources/demo_workflow.yml` — end-to-end Workflow die alles aan elkaar knoopt
9. `demo_showcase/` notebooks — Time Travel, Audit Logs, Lineage
10. `docs/demo_script.md` — handmatig demo-draaiboek schrijven
11. *(Later)* Integration-laag uitwerken
12. *(Later)* Datamart-laag uitwerken

---

## 11. Belangrijke Adviezen

- **Gebruik Databricks Widgets** bovenaan elke notebook voor omgeving-selectie (`DEV`/`TEST`/`PROD`)
- **Schrijf elke notebook idempotent** — meerdere keren draaien geeft hetzelfde resultaat
- **Voeg row counts toe** na elke laadstap — klanten willen zien dat data aankomt
- **Gebruik `%md` cellen rijkelijk** — de notebook is ook het demo-document
- **Orchestratie loopt via een Databricks Workflow**, gedefinieerd in `resources/demo_workflow.yml`. `dbutils.notebook.run()` kan geen DLT- of Lakeflow Connect-pipelines triggeren; die hebben een Workflow met `pipeline_task` nodig. De Workflow is ook zelf een demo-moment (graph view, taakstatus, retries).
