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
- Submappen per tabel binnen het volume

```
/Volumes/demo_dev/staging_azurestorage/source/
├── order_header/
└── order_detail/
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
| `source_path` | string | `/Volumes/demo_dev/staging_azurestorage/source/order_header` | Pad naar de data |
| `target_schema` | string | `STAGING_AZURESTORAGE` | Doelschema |
| `target_table` | string | `order_header` | Doeltabelnaam |
| `file_format` | string | `parquet` | Bestandstype |
| `is_active` | boolean | `true` | Aan/uit zonder rij te verwijderen |
| `load_type` | string | `full` | `full` of `incremental` |

### Initiële vulling

| source_system | target_table | load_type |
|---|---|---|
| `azurestorage` | `order_header` | `full` |
| `azurestorage` | `order_detail` | `full` |
| `sqlserver` | *(onbekend)* | `full` |

**Argumentatie:** De control table is een Delta-tabel en profiteert daarmee automatisch van Delta Time Travel, Unity Catalog Audit Logs en Lineage — zonder extra configuratie.

---

## 5. Audit-kolommen (elke staging-tabel)

Elke tabel in de staging-laag krijgt standaard de volgende metadata-kolommen:

| Kolom | Inhoud |
|---|---|
| `_ingestion_timestamp` | `current_timestamp()` |
| `_source_system` | `azurestorage` of `sqlserver` |
| `_source_file` | Bestandsnaam of tabelnaam van de bron |
| `_last_modified` | Laatste wijzigingstijd van het bronbestand |
| `_pipeline_run_id` | ID van de Databricks job run |

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
| `(sql tabel)` | Basis (notebook) |
| `(sql tabel)_dlt` | Delta Live Tables |

---

## 7. Mapstructuur

```
demo_databricks/
├── config/
│   └── 00_setup_control_table.py       # Control table aanmaken & vullen
├── staging/
│   ├── 01_create_volumes.py            # Volumes aanmaken
│   ├── 02_ingest_azurestorage.py       # Parquet inladen via control table
│   ├── 03_ingest_sqlserver.py          # SQL Server inladen via control table
│   └── dlt/
│       └── 04_staging_dlt_pipeline.py  # DLT versie van staging
├── integration/
│   └── (later uit te werken)
├── datamart/
│   └── (later uit te werken)
└── demo_showcase/
    ├── delta_time_travel.py            # Delta Time Travel demo
    ├── audit_logs.py                   # Audit Logs demo
    └── lineage.py                      # Lineage demo
```

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

1. Git-repo koppelen aan Databricks Repos
2. `00_setup_control_table.py` — catalog, schema, control table aanmaken
3. `01_create_volumes.py` — volume aanmaken en koppelen aan Azure Storage
4. `02_ingest_azurestorage.py` — parquet inladen, Delta-tabellen schrijven
5. `03_ingest_sqlserver.py` — SQL Server verbinding en inladen
6. `04_staging_dlt_pipeline.py` — DLT pipeline bouwen
7. `demo_showcase/` notebooks — Time Travel, Audit Logs, Lineage
8. *(Later)* Integration-laag uitwerken
9. *(Later)* Datamart-laag uitwerken

---

## 11. Belangrijke Adviezen

- **Gebruik Databricks Widgets** bovenaan elke notebook voor omgeving-selectie (`DEV`/`TEST`/`PROD`)
- **Schrijf elke notebook idempotent** — meerdere keren draaien geeft hetzelfde resultaat
- **Voeg row counts toe** na elke laadstap — klanten willen zien dat data aankomt
- **Gebruik `%md` cellen rijkelijk** — de notebook is ook het demo-document
- **Maak één master notebook** die alle andere aanroept via `dbutils.notebook.run()`
