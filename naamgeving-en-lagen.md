# Naamgeving en Lagen — Instructies voor Claude Code

> **Note (2026-05-15):** This document is now a cheat-sheet of layer + naming primitives. Binding decisions on SCD1/SCD2 mechanics, DW/DWH/DWQ shape, fact source, and admin column semantics live in `docs/adr/0010-*.md` through `docs/adr/0020-*.md`. The pruned sections below cross-reference the ADR that governs each topic. Earlier §2.6 SCD1 formulas have been removed — see [ADR-0012](docs/adr/0012-scd1-dim-views-source-from-latest-row.md).

> **Doel van dit document**: oorspronkelijk dé bron van waarheid voor naamgeving en layering. Vandaag een naslagdocument voor de KRM-primitieven (`SA_*`, `WA_*`, `WK_`, `WKR_`, `MK_`, `MA_*`, etc.); voor bindende ontwerpbeslissingen prevaleren de ADR's.

---

## 1. Architectuur in één oogopslag — zie ADR-0010, ADR-0011, ADR-0016, ADR-0020

```
┌──────────────────────┐    ┌──────────────────────┐    ┌──────────────────────┐
│  STAGING             │    │  INTEGRATION         │    │  DATAMART            │
│  demo.staging_<bron> │───▶│  demo.integration    │───▶│  demo.datamart       │
│                      │    │                      │    │                      │
│  STG_<TABEL>         │    │  DW_<TABEL>  (tabel) │    │  DIM_<NAAM>  (view)  │
│  Delta + MERGE       │    │  DWH_<TABEL> (view)  │    │  FCT_<NAAM>  (table) │
│                      │    │  DWQ_<TABEL> (tabel) │    │                      │
└──────────────────────┘    └──────────────────────┘    └──────────────────────┘
```

- DW/DWH-mechaniek (`APPLY CHANGES INTO STORED AS SCD TYPE 2`, beheer van `__START_AT`/`__END_AT`, rename naar `WA_FROMDATE`/`WA_UNTODATE`): zie [ADR-0010](docs/adr/0010-dw-captures-history-via-apply-changes-into.md).
- Quality-routing naar `DWQ_<TABEL>`: zie [ADR-0011](docs/adr/0011-quality-failed-rows-route-to-paired-dwq-table.md).
- Eén canonieke `DW_<TABEL>` per entiteit (niet per bron): zie [ADR-0016](docs/adr/0016-integration-entities-are-canonical-not-per-source.md).
- Pipeline-topologie (Workflow over per-layer DLT pipelines, `DIM_*` als plain UC-views): zie [ADR-0020](docs/adr/0020-pipeline-topology-one-workflow-over-per-layer-dlt-pipelines.md).

---

## 2. Naamgevingsregels

### 2.1 Catalog, schema en omgevingen

| Niveau | Regel | Voorbeeld |
|---|---|---|
| Catalog | Per project/klant, kleine letters | `demo` |
| Schema staging | `staging_<bronsysteem>` (kleine letters) | `staging_ms_crm`, `staging_azurestorage` |
| Schema integration | Vast: `integration` | `integration` |
| Schema datamart | Vast: `datamart` | `datamart` |

Multi-environment (dev/tst/prod) wordt **niet** via catalog-naam afgehandeld in dit document — dat regelt **Databricks Asset Bundles (DAB)** via variabele-substitutie. De notebooks zelf zijn omgeving-agnostisch; ze lezen de catalog uit een widget.

### 2.2 Object-naamgeving (tabellen, views, kolommen) — zie ADR-0017

| Object | Prefix | Casing | Voorbeeld |
|---|---|---|---|
| Staging tabel | `STG_` | HOOFDLETTERS | `STG_KLANT` |
| DW historische tabel | `DW_` | HOOFDLETTERS | `DW_KLANT` |
| DWH historische view | `DWH_` | HOOFDLETTERS | `DWH_KLANT` |
| DWQ quarantine tabel | `DWQ_` | HOOFDLETTERS | `DWQ_KLANT` |
| Datamart dimensie view | `DIM_` | HOOFDLETTERS | `DIM_KLANT` |
| Datamart fact tabel | `FCT_` | HOOFDLETTERS | `FCT_SALES` |

**Regel voor kolomnamen**:
- **Administratie- en sleutelvelden**: HOOFDLETTERS volgens onderstaande prefixen
- **Business-kolommen uit bron**: behoud exact de casing zoals in bron (`klantcode`, `Klantnaam`, `boekdatum`)

Voor Lakeflow Connect-bronnen geldt een uitzondering op de `SA_*`-kolommen: zie [ADR-0017](docs/adr/0017-lakeflow-connect-staging-tables-do-not-carry-sa-admin-columns.md).

### 2.3 Veld-prefixen — zie ADR-0013, ADR-0014, ADR-0019

| Prefix | Laag | Betekenis | Voorbeeld |
|---|---|---|---|
| `WK_<TABEL>` | Integration | Unieke surrogaatsleutel (identity) van entiteit `<TABEL>` | `WK_KLANT` |
| `WK_REF_BUS_<REF_TABEL>` | Integration | Business key van gerefereerde tabel `<REF_TABEL>` | `WK_REF_BUS_MEDEWERKER` |
| `WK_REF_HASH_<REF_TABEL>` | Integration | Hash (SHA2-256) van business key van gerefereerde tabel | `WK_REF_HASH_MEDEWERKER` |
| `WKP_<TABEL>` | Integration (DWH-view) | Vorige versie van `WK_<TABEL>` (LAG) | `WKP_KLANT` |
| `WKR_<TABEL>` | Integration (DWH-view) | Eerste versie van `WK_<TABEL>` (FIRST_VALUE) | `WKR_KLANT` |
| `WA_*` | Integration | Administratieveld DW-laag | zie 2.5 |
| `SA_*` | Staging | Administratieveld Staging-laag | zie 2.4 |
| `MK_<NAAM>` | Datamart | Datamart sleutel (FK naar dimensie) | `MK_KLANT` |
| `MA_*` | Datamart | Administratieveld datamart | zie ADR-0012 / ADR-0013 |

- Geen self-side `WKR_HASH_<TABEL>` in DWH-views: zie [ADR-0014](docs/adr/0014-no-self-side-bk-hash-column-in-dwh-views.md).
- Alle hashes gebruiken `SHA2(<input>, 256)`: zie [ADR-0019](docs/adr/0019-all-hash-columns-use-sha2-256.md).

### 2.4 Verplichte administratie-kolommen — Staging (Auto Loader-bronnen)

Elke `STG_<TABEL>` die via Auto Loader binnenkomt heeft de volgende `SA_*` kolommen:

| Kolom | Type | Betekenis |
|---|---|---|
| `SA_CRUDDTS` | TIMESTAMP | Moment waarop record in Staging is geladen |
| `SA_SRC` | STRING | Bronsysteem identificatie (matcht `source_system` in YAML) |
| `SA_RUNID` | STRING | ETL run identifier (komt uit widget) |

Voor Lakeflow Connect-bronnen ontbreken deze kolommen — daar worden `_change_type`, `_change_version`, `_commit_timestamp` van LC direct in de integration-laag vertaald naar `WA_*`. Bindende spec: [ADR-0017](docs/adr/0017-lakeflow-connect-staging-tables-do-not-carry-sa-admin-columns.md).

### 2.5 Verplichte administratie-kolommen — DW (Integration) — zie ADR-0010, ADR-0015

`__START_AT` en `__END_AT` worden door `APPLY CHANGES INTO` beheerd; in de `DWH_<TABEL>` view worden ze hernoemd naar de Axians-conventies.

**Op DW_-tabel zelf** (gevoed door `APPLY CHANGES INTO`):

| Kolom | Type | Betekenis |
|---|---|---|
| `WK_<TABEL>` | BIGINT (Identity) | Surrogaatsleutel, `GENERATED ALWAYS AS IDENTITY` |
| `__START_AT` | TIMESTAMP | Door Databricks beheerd — wordt `WA_FROMDATE` in view |
| `__END_AT` | TIMESTAMP | Door Databricks beheerd — wordt `WA_UNTODATE` in view |
| `WA_CRUDDTS` | TIMESTAMP | Moment van laden in DW |
| `WA_CRUD` | STRING(1) | `C`reate / `U`pdate / `D`elete (uit CDF `_change_type`) |
| `WA_SRC` | STRING | Bronsysteem (per-row provenance — zie ADR-0016) |
| `WA_RUNID` | STRING | ETL run identifier |
| `WA_HASH` | STRING | SHA2-256 van non-BK business-kolommen — bedoeld voor reconciliation, niet voor change-detection (zie [ADR-0015](docs/adr/0015-wa-hash-is-kept-for-source-reconciliation-not-change-detection.md)) |
| _business kolommen_ | _div._ | Alle entity-kolommen |
| `WK_REF_BUS_<REF>` | _als bron-BK_ | Per `foreign_keys`-relatie |
| `WK_REF_HASH_<REF>` | STRING | SHA2-256 van de BK van gerefereerde tabel |

**In DWH_<TABEL>-view** (bovenop DW_<TABEL>):

| Kolom | Bron | Betekenis |
|---|---|---|
| Alle kolommen van `DW_<TABEL>` | — | Doorgegeven |
| `WA_FROMDATE` | `__START_AT` | Aanvang geldigheid |
| `WA_UNTODATE` | `COALESCE(__END_AT, TIMESTAMP '9999-12-31 00:00:00')` | Einde geldigheid |
| `WA_ISCURR` | `CASE WHEN __END_AT IS NULL THEN 1 ELSE 0 END` | Huidige versie indicator |
| `WKP_<TABEL>` | `LAG(WK_<TABEL>) OVER (PARTITION BY <BK> ORDER BY __START_AT)` | Vorige versie |
| `WKR_<TABEL>` | `FIRST_VALUE(WK_<TABEL>) OVER (PARTITION BY <BK> ORDER BY __START_AT)` | Eerste versie |

### 2.6 SCD1-formules — zie [ADR-0012](docs/adr/0012-scd1-dim-views-source-from-latest-row.md) voor bindende spec

---

## 3. Input: YAML structuur — zie ADR-0009 (informationeel)

> **Note**: [ADR-0009](docs/adr/0009-entity-structure-lives-in-the-sql.md) verbiedt een machine-leesbaar entity-registry. Entity-structuur (kolommen, BK's, FK's, datamart-inclusie) leeft in de DLT-SQL en in `CONTEXT.md`. Deze sectie is bewaard voor historische context; YAML-gedreven codegeneratie is **niet** de huidige aanpak.

### 3.1 Bron-tabel YAML — `bronnen/<bronsysteem>/<tabel>.yml`

```yaml
source_system: ms_crm                  # → staging_ms_crm schema + SA_SRC/WA_SRC waarde
source_table: klant                    # → exacte tabelnaam in bron
staging_table: STG_KLANT               # → naam in staging-laag
integration_table: DW_KLANT            # → naam in integration-laag (DWH_ volgt automatisch)

business_keys:
  - klantcode

sequence_by: SA_CRUDDTS                # → bepaalt volgorde voor APPLY CHANGES

track_history_columns: ALL_EXCEPT_BK   # ALL_EXCEPT_BK | of expliciete lijst van kolommen

columns:
  - name: klantcode
    type: STRING
    nullable: false
    description: "Unieke klantcode in CRM"
  - name: klantnaam
    type: STRING
    nullable: false
  - name: ingangsdatum
    type: DATE
    nullable: true

foreign_keys:                          # optioneel — alleen als entiteit FKs heeft
  - column: accountmanager_id
    references_table: medewerker
    references_bk: medewerkernummer

datamart:                              # optioneel — alleen als entiteit naar datamart gaat
  generate_dim: true
  scd_type: 2                          # 1 of 2
  dim_name: DIM_KLANT
```

### 3.2 Fact YAML — `datamart/<fct_naam>.yml`

```yaml
fact_name: FCT_SALES
grain: "Eén rij per verkoopregel per boekdatum"

source:
  primary: DWH_SALES_LINE              # facts lezen uit DWH_ (ADR-0020), niet uit DIM_
  joins:                               # optioneel — extra DWH_-tabellen mee-joinen
    - table: DWH_SALES_ORDER
      on: order_id

dimensions:                            # FK-resolutie op WKR_<REF> uit DWH_
  - dim: DIM_KLANT
    fact_column: MK_KLANT
    join_on: klant_id                  # BK-kolom in source
  - dim: DIM_PRODUCT
    fact_column: MK_PRODUCT
    join_on: product_id
  - dim: DIM_DATE                      # zie ADR-0018
    fact_column: MK_BOEKDAT
    join_on: boekdatum

measures:
  - name: OMZET
    source_column: regel_totaal
    type: DECIMAL(18,2)
  - name: AANTAL
    source_column: aantal
    type: INT

incremental:
  strategy: append                     # append | merge | overwrite
  watermark_column: WA_CRUDDTS         # bij append/merge
```

---

## 4. Notebook-structuur

Elke notebook is **één Python `.ipynb`** in `notebooks/<laag>/<lowercase_tabelnaam>.ipynb` met de volgende vaste structuur:

1. **Markdown header cell**: titel + verwijzing naar bron-SQL/CONTEXT.md
2. **Python cell**: widget-definities + lezen van parameters
3. **Markdown cell** per object dat aangemaakt wordt
4. **`%sql` cell** per `CREATE STREAMING TABLE` / `CREATE OR REFRESH STREAMING TABLE` / `CREATE OR REPLACE VIEW` statement

### 4.1 Verplichte widgets in elke notebook

```python
dbutils.widgets.text("catalog", "demo")
dbutils.widgets.text("source_system", "")    # leeg laten voor integration/datamart
dbutils.widgets.text("run_id", "")

catalog = dbutils.widgets.get("catalog")
source_system = dbutils.widgets.get("source_system")
run_id = dbutils.widgets.get("run_id")
```

---

## 5. Werkende voorbeelden per laag

### 5.1 Staging — `notebooks/staging/stg_klant.ipynb`

Auto Loader-bron. Voor Lakeflow Connect-bronnen wijken de admin-kolommen af (zie [ADR-0017](docs/adr/0017-lakeflow-connect-staging-tables-do-not-carry-sa-admin-columns.md)).

**Cell 4 (`%sql`)**:
```sql
%sql
CREATE OR REFRESH STREAMING TABLE ${c.catalog}.staging_${c.source_system}.STG_KLANT
(
  CONSTRAINT valid_klantcode EXPECT (klantcode IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_klantnaam EXPECT (klantnaam IS NOT NULL) ON VIOLATION DROP ROW
)
TBLPROPERTIES (
  delta.enableChangeDataFeed = 'true'
)
COMMENT 'Staging klant — incrementele MERGE op business key klantcode';

-- APPLY CHANGES INTO doet hier een upsert (geen SCD2 in staging)
APPLY CHANGES INTO ${c.catalog}.staging_${c.source_system}.STG_KLANT
FROM STREAM read_files('/Volumes/${c.catalog}/landing/${c.source_system}/klant/',
                       format => 'parquet')
KEYS (klantcode)
SEQUENCE BY ingangsdatum
COLUMNS klantcode, klantnaam, ingangsdatum,
        current_timestamp() AS SA_CRUDDTS,
        '${c.source_system}' AS SA_SRC,
        '${c.run_id}' AS SA_RUNID
STORED AS SCD TYPE 1;
```

> **Let op**: De **geschiedenis** wordt pas opgebouwd in de DW-laag — zie [ADR-0010](docs/adr/0010-dw-captures-history-via-apply-changes-into.md).

### 5.2 Integration — `notebooks/integration/dw_klant.ipynb`

Bevat `DW_KLANT` (streaming table met SCD2) én `DWH_KLANT` (view). Bindende mechaniek: [ADR-0010](docs/adr/0010-dw-captures-history-via-apply-changes-into.md). Bij quality-failures route via tagged MV naar `DWQ_KLANT`: [ADR-0011](docs/adr/0011-quality-failed-rows-route-to-paired-dwq-table.md).

**Cell 3 (`%sql` — DW_KLANT)**:
```sql
%sql
CREATE OR REFRESH STREAMING TABLE ${c.catalog}.integration.DW_KLANT
(
  WK_KLANT       BIGINT GENERATED ALWAYS AS IDENTITY,
  klantcode      STRING NOT NULL,
  klantnaam      STRING,
  ingangsdatum   DATE,
  WA_CRUDDTS     TIMESTAMP,
  WA_CRUD        STRING,
  WA_SRC         STRING,
  WA_RUNID       STRING,
  WA_HASH        STRING,
  CONSTRAINT valid_bk EXPECT (klantcode IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT 'Historische laag klant — SCD2 via APPLY CHANGES INTO';

-- SCD2 op basis van Staging CDF
APPLY CHANGES INTO ${c.catalog}.integration.DW_KLANT
FROM STREAM(
  SELECT
    klantcode,
    klantnaam,
    ingangsdatum,
    current_timestamp() AS WA_CRUDDTS,
    CASE _change_type
      WHEN 'insert'           THEN 'C'
      WHEN 'update_postimage' THEN 'U'
      WHEN 'delete'           THEN 'D'
    END AS WA_CRUD,
    '${c.source_system}' AS WA_SRC,
    '${c.run_id}' AS WA_RUNID,
    sha2(concat_ws('||', coalesce(klantnaam, ''), coalesce(cast(ingangsdatum as string), '')), 256) AS WA_HASH,
    _commit_timestamp
  FROM STREAM table_changes('${c.catalog}.staging_${c.source_system}.STG_KLANT', 0)
  WHERE _change_type IN ('insert', 'update_postimage', 'delete')
)
KEYS (klantcode)
APPLY AS DELETE WHEN _change_type = 'delete'
SEQUENCE BY _commit_timestamp
COLUMNS klantcode, klantnaam, ingangsdatum,
        WA_CRUDDTS, WA_CRUD, WA_SRC, WA_RUNID, WA_HASH
STORED AS SCD TYPE 2;
```

**Cell 4 (`%sql` — DWH_KLANT view)**:
```sql
%sql
CREATE OR REPLACE VIEW ${c.catalog}.integration.DWH_KLANT AS
SELECT
  WK_KLANT,
  LAG(WK_KLANT) OVER (PARTITION BY klantcode ORDER BY __START_AT)         AS WKP_KLANT,
  FIRST_VALUE(WK_KLANT) OVER (PARTITION BY klantcode ORDER BY __START_AT) AS WKR_KLANT,
  klantcode,
  klantnaam,
  ingangsdatum,
  WA_CRUDDTS,
  WA_CRUD,
  WA_SRC,
  WA_RUNID,
  WA_HASH,
  __START_AT                                                       AS WA_FROMDATE,
  COALESCE(__END_AT, TIMESTAMP '9999-12-31 00:00:00')              AS WA_UNTODATE,
  CASE WHEN __END_AT IS NULL THEN 1 ELSE 0 END                     AS WA_ISCURR
FROM ${c.catalog}.integration.DW_KLANT;
```

### 5.3 Datamart Type 2 — `notebooks/datamart/dim_klant.ipynb`

Voor `scd_type: 2`. Bindende spec: [ADR-0013](docs/adr/0013-scd2-dim-views-expose-every-version.md) (toont elke versie; `MK_<NAAM>` ← `WK_<TABEL>`; `MK_ROOT` ← `WKR_<TABEL>`).

**Cell 3 (`%sql` — DIM_KLANT view)**:
```sql
%sql
CREATE OR REPLACE VIEW ${c.catalog}.datamart.DIM_KLANT AS
SELECT
  WK_KLANT      AS MK_KLANT,
  WKR_KLANT     AS MK_ROOT,
  klantcode,
  klantnaam,
  ingangsdatum,
  WA_FROMDATE   AS MA_FROM,
  WA_UNTODATE   AS MA_UNTO,
  WA_ISCURR     AS MA_ISCURR
FROM ${c.catalog}.integration.DWH_KLANT;
```

### 5.4 Datamart Type 1 — `notebooks/datamart/dim_product.ipynb`

Voor `scd_type: 1`. Bindende spec: [ADR-0012](docs/adr/0012-scd1-dim-views-source-from-latest-row.md). Drie correcties t.o.v. eerdere §2.6:
- Source = **latest row per BK by `WA_FROMDATE`**, niet `WA_ISCURR = 1` (anders verdwijnen verwijderde entiteiten);
- `MK_<NAAM>` = `WKR_<TABEL>` (root surrogaat, stabiel over alle versies), niet `WK_<TABEL>`;
- `MA_CHANGEDATE` = `MAX(WA_CRUDDTS)` waar `WA_CRUD <> 'C'` (updates **en** deletes tellen mee), niet alleen `WA_CRUD = 'U'`.

**Cell 3 (`%sql` — DIM_PRODUCT view)**:
```sql
%sql
CREATE OR REPLACE VIEW ${c.catalog}.datamart.DIM_PRODUCT AS
WITH ranked AS (
  SELECT
    d.*,
    ROW_NUMBER() OVER (PARTITION BY productcode ORDER BY WA_FROMDATE DESC) AS rn
  FROM ${c.catalog}.integration.DWH_PRODUCT d
),
historie AS (
  SELECT
    productcode,
    MIN(WA_CRUDDTS)                                              AS MA_CREATEDATE,
    MAX(CASE WHEN WA_CRUD <> 'C' THEN WA_CRUDDTS END)            AS MA_CHANGEDATE
  FROM ${c.catalog}.integration.DWH_PRODUCT
  GROUP BY productcode
)
SELECT
  r.WKR_PRODUCT  AS MK_PRODUCT,
  r.productcode,
  r.productnaam,
  h.MA_CREATEDATE,
  h.MA_CHANGEDATE,
  CASE WHEN r.WA_UNTODATE <> TIMESTAMP '9999-12-31 00:00:00' THEN 1 ELSE 0 END AS MA_ISDEL
FROM ranked r
JOIN historie h ON h.productcode = r.productcode
WHERE r.rn = 1;
```

### 5.5 Fact tabel — `notebooks/datamart/fct_sales.ipynb`

Facts lezen direct uit `DWH_<TABEL>` en projecteren `WKR_<TABEL>` als `MK_<NAAM>`, zodat de fact-build niet afhangt van de DIM-views (zie [ADR-0020](docs/adr/0020-pipeline-topology-one-workflow-over-per-layer-dlt-pipelines.md)). `DIM_DATE` is een view met `MK_DATE = yyyymmdd::INT` (zie [ADR-0018](docs/adr/0018-dim-date-is-a-generated-calendar-view-keyed-by-yyyymmdd.md)).

**Cell 3 (`%sql` — FCT_SALES streaming table)**:
```sql
%sql
CREATE OR REFRESH STREAMING TABLE ${c.catalog}.datamart.FCT_SALES
(
  MK_BOEKDAT     INT,
  MK_KLANT       BIGINT,
  MK_PRODUCT     BIGINT,
  OMZET          DECIMAL(18,2),
  AANTAL         INT,
  WA_CRUDDTS     TIMESTAMP
)
COMMENT 'Verkoopfeiten — grain: één rij per verkoopregel per boekdatum';

APPLY CHANGES INTO ${c.catalog}.datamart.FCT_SALES
FROM STREAM(
  SELECT
    -- date dimensie: directe join op datum-key (yyyymmdd::INT — ADR-0018)
    date_format(s.boekdatum, 'yyyyMMdd')::INT AS MK_BOEKDAT,

    -- klant dimensie: WKR_ uit DWH levert het stabiele root-surrogaat
    k.WKR_KLANT  AS MK_KLANT,

    -- product dimensie: WKR_ uit DWH
    p.WKR_PRODUCT AS MK_PRODUCT,

    -- measures
    s.regel_totaal AS OMZET,
    s.aantal       AS AANTAL,

    s.WA_CRUDDTS,
    s._commit_timestamp
  FROM STREAM table_changes('${c.catalog}.integration.DWH_SALES_LINE', 0) s
  LEFT JOIN ${c.catalog}.integration.DWH_KLANT k
    ON k.klantcode = s.klant_id
   AND s.boekdatum >= k.WA_FROMDATE
   AND s.boekdatum <  k.WA_UNTODATE
  LEFT JOIN ${c.catalog}.integration.DWH_PRODUCT p
    ON p.productcode = s.product_id
   AND s.boekdatum >= p.WA_FROMDATE
   AND s.boekdatum <  p.WA_UNTODATE
  WHERE s._change_type IN ('insert', 'update_postimage')
)
KEYS (MK_BOEKDAT, MK_KLANT, MK_PRODUCT)
SEQUENCE BY _commit_timestamp
STORED AS SCD TYPE 1;
```

> **Belangrijk**: De temporele join gebruikt `>= WA_FROMDATE AND < WA_UNTODATE` (half-open interval) om dubbele matches op grensmomenten te voorkomen.

---

## 6. Regels voor Claude Code bij genereren

> **Note**: De codegen-georiënteerde regels (YAML → notebooks) hieronder zijn behouden voor historische context. [ADR-0009](docs/adr/0009-entity-structure-lives-in-the-sql.md) verbiedt een YAML-gedreven entity-registry; entity-structuur leeft in de DLT-SQL. Lees deze sectie daarom als naslag, niet als bindende workflow.

### 6.1 Algemeen
1. **Comments en markdown in het Nederlands**. Technische trefwoorden (`MERGE`, `APPLY CHANGES INTO`, `STREAMING TABLE`) blijven Engels.
2. **Serverless-compatibel blijven**: geen RDD API, geen `dbutils.fs.mount`, geen Spark-config wijzigingen die serverless niet ondersteunt. Naamgeving §1's eis "alles serverless DLT" is op twee plekken versoepeld; zie [ADR-0020](docs/adr/0020-pipeline-topology-one-workflow-over-per-layer-dlt-pipelines.md).

### 6.2 Schema-naamgeving
- Staging schema: altijd `staging_<source_system>` (lowercase).
- Integration schema: altijd `integration`.
- Datamart schema: altijd `datamart`.
- Catalog: altijd via widget — nooit hardcoden.

### 6.3 Bij iedere `CREATE STREAMING TABLE`
- Zet `delta.enableChangeDataFeed = 'true'` als de tabel downstream gelezen wordt door een andere streaming table.
- Zet altijd een `COMMENT` met korte beschrijving van de tabel.
- Quality-checks op staging worden vermeden — zie [ADR-0011](docs/adr/0011-quality-failed-rows-route-to-paired-dwq-table.md) (en [ADR-0007](docs/adr/0007-quality-issues-are-routed-not-silently-dropped.md)).

### 6.4 Bij iedere `APPLY CHANGES INTO`
- `KEYS` = business keys uit entity-spec.
- `SEQUENCE BY` = de juiste timestamp (staging-load voor staging; `_commit_timestamp` voor integration uit staging CDF).
- `STORED AS SCD TYPE 1` voor staging (huidige stand per BK).
- `STORED AS SCD TYPE 2` voor integration (volledige historie) — zie [ADR-0010](docs/adr/0010-dw-captures-history-via-apply-changes-into.md).
- `APPLY AS DELETE WHEN _change_type = 'delete'` om source-deletes te honoreren.

### 6.5 Bij iedere DWH-view
- Voeg verplicht toe: `WA_FROMDATE`, `WA_UNTODATE`, `WA_ISCURR`, `WKP_<TABEL>`, `WKR_<TABEL>`.
- Geef alle business- en `WA_*`-kolommen door uit DW_-tabel.
- Geen self-side `WKR_HASH_` (zie [ADR-0014](docs/adr/0014-no-self-side-bk-hash-column-in-dwh-views.md)).

### 6.6 Bij iedere DIM-view
- Type 2: zie [ADR-0013](docs/adr/0013-scd2-dim-views-expose-every-version.md) — elke versie, `MK_<NAAM>` ← `WK_`, `MK_ROOT` ← `WKR_`.
- Type 1: zie [ADR-0012](docs/adr/0012-scd1-dim-views-source-from-latest-row.md) — latest-row-per-BK, `MK_<NAAM>` ← `WKR_`, `MA_CHANGEDATE` includeert deletes.
- `DIM_DATE` is een gegenereerde calendar-view, geen entity-dimensie — zie [ADR-0018](docs/adr/0018-dim-date-is-a-generated-calendar-view-keyed-by-yyyymmdd.md).

### 6.7 Bij iedere FCT-tabel
- Facts lezen uit `DWH_<TABEL>` (niet uit `DIM_`) en projecteren `WKR_<REF>` als `MK_<NAAM>` — zie [ADR-0020](docs/adr/0020-pipeline-topology-one-workflow-over-per-layer-dlt-pipelines.md).
- Per dimensie: temporele join op `WA_FROMDATE`/`WA_UNTODATE` van de DWH-versie waarvan de fact-rij afhangt; directe join op `yyyymmdd::INT` voor `DIM_DATE`.

---

## 7. Folder-structuur

```
.
├── README.md
├── databricks.yml                      # DAB config (multi-env)
├── CONTEXT.md                          # human-readable entity-inventory (zie ADR-0009)
├── docs/adr/                           # binding ontwerpbeslissingen (ADR-0001…ADR-0020)
├── naamgeving-en-lagen.md              # ← dit document (cheat-sheet)
├── integration/                        # DLT-SQL voor DW_/DWH_/DWQ_-entiteiten
├── datamart/                           # DLT-SQL voor FCT_-tabellen
├── views/datamart/                     # apply_views: plain UC DIM_-views
├── resources/
│   └── demo_workflow.yml               # Workflow over per-layer pipelines (ADR-0020)
└── notebooks/                          # staging Auto Loader + ondersteunende tasks
    ├── staging/
    ├── integration/                    # (legacy/voorbeelden)
    └── datamart/                       # (legacy/voorbeelden)
```

---

## 8. Snelle referentie — beslissingsboom

```
Nieuwe entiteit (in DLT-SQL — ADR-0009):
│
├─▶ Staging: STG_<TABEL> via Auto Loader (of via LC voor SQL Server — ADR-0017)
│
├─▶ Integration: DW_<TABEL> via APPLY CHANGES INTO ... SCD TYPE 2 (ADR-0010)
│   + DWQ_<TABEL> via tagged MV (ADR-0011)
│   + DWH_<TABEL> view met WA_FROMDATE/WA_UNTODATE/WA_ISCURR/WKP_/WKR_
│
└─▶ Datamart vereist?
    ├─▶ SCD2: DIM_<NAAM> view die elke versie toont (ADR-0013)
    └─▶ SCD1: DIM_<NAAM> view = latest-row-per-BK uit DWH_ (ADR-0012)

Nieuwe FCT_<NAAM>:
│
└─▶ Streaming table die DWH_<TABEL> leest en WKR_<REF> projecteert als MK_<NAAM>
    Date-dimensie: directe join op yyyymmdd::INT (DIM_DATE — ADR-0018)
    Entiteits-dimensies: temporeel op WA_FROMDATE/WA_UNTODATE
```
