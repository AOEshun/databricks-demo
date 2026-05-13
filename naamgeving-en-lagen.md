# Naamgeving en Lagen — Instructies voor Claude Code

> **Doel van dit document**: Claude Code gebruikt dit bestand als enige bron van waarheid bij het genereren van Databricks notebooks (`.ipynb`) voor het opbouwen van Staging-, Integration- en Datamart-lagen. Alle regels in dit document zijn bindend. Wijk er niet van af zonder expliciete instructie van de gebruiker.

---

## 1. Architectuur in één oogopslag

```
┌──────────────────────┐    ┌──────────────────────┐    ┌──────────────────────┐
│  STAGING             │    │  INTEGRATION         │    │  DATAMART            │
│  demo.staging_<bron> │───▶│  demo.integration    │───▶│  demo.datamart       │
│                      │    │                      │    │                      │
│  STG_<TABEL>         │    │  DW_<TABEL>  (tabel) │    │  DIM_<NAAM>  (view)  │
│  Delta + MERGE       │    │  DWH_<TABEL> (view)  │    │  FCT_<NAAM>  (table) │
└──────────────────────┘    └──────────────────────┘    └──────────────────────┘
```

**Deployment**: Alles draait in **Lakeflow Declarative Pipelines (DLT)** op **serverless compute**. Claude Code mag geen constructies gebruiken die buiten serverless werken (geen RDD API, geen DBFS root mounts, geen init scripts).

**Input voor Claude Code**: YAML bestanden in `bronnen/<bronsysteem>/<tabel>.yml` en `datamart/<fct_naam>.yml`.

**Output van Claude Code**: Python `.ipynb` notebooks met `%sql` magic cells in `notebooks/{staging,integration,datamart}/`.

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

### 2.2 Object-naamgeving (tabellen, views, kolommen)

| Object | Prefix | Casing | Voorbeeld |
|---|---|---|---|
| Staging tabel | `STG_` | HOOFDLETTERS | `STG_KLANT` |
| DW historische tabel | `DW_` | HOOFDLETTERS | `DW_KLANT` |
| DWH historische view | `DWH_` | HOOFDLETTERS | `DWH_KLANT` |
| Datamart dimensie view | `DIM_` | HOOFDLETTERS | `DIM_KLANT` |
| Datamart fact tabel | `FCT_` | HOOFDLETTERS | `FCT_SALES` |

**Regel voor kolomnamen**:
- **Administratie- en sleutelvelden**: HOOFDLETTERS volgens onderstaande prefixen
- **Business-kolommen uit bron**: behoud exact de casing zoals in bron (`klantcode`, `Klantnaam`, `boekdatum`)

### 2.3 Veld-prefixen

| Prefix | Laag | Betekenis | Voorbeeld |
|---|---|---|---|
| `WK_<TABEL>` | Integration | Unieke surrogaatsleutel (identity) van entiteit `<TABEL>` | `WK_KLANT` |
| `WK_REF_BUS_<REF_TABEL>` | Integration | Business key van gerefereerde tabel `<REF_TABEL>` | `WK_REF_BUS_MEDEWERKER` |
| `WK_REF_HASH_<REF_TABEL>` | Integration | Hash van business key van gerefereerde tabel | `WK_REF_HASH_MEDEWERKER` |
| `WKP_<TABEL>` | Integration (DWH-view) | Vorige versie van `WK_<TABEL>` (LAG) | `WKP_KLANT` |
| `WKR_<TABEL>` | Integration (DWH-view) | Eerste versie van `WK_<TABEL>` (FIRST_VALUE) | `WKR_KLANT` |
| `WA_*` | Integration | Administratieveld DW-laag | zie 2.4 |
| `SA_*` | Staging | Administratieveld Staging-laag | zie 2.4 |
| `MK_<TABEL>` | Datamart | Datamart sleutel (FK naar dimensie) | `MK_KLANT` |
| `MA_*` | Datamart | Administratieveld datamart | zie 2.6 |

### 2.4 Verplichte administratie-kolommen — Staging

Elke `STG_<TABEL>` heeft de volgende `SA_*` kolommen:

| Kolom | Type | Betekenis |
|---|---|---|
| `SA_CRUDDTS` | TIMESTAMP | Moment waarop record in Staging is geladen |
| `SA_SRC` | STRING | Bronsysteem identificatie (matcht `source_system` in YAML) |
| `SA_RUNID` | STRING | ETL run identifier (komt uit widget) |

### 2.5 Verplichte administratie-kolommen — DW (Integration)

Elke `DW_<TABEL>` heeft minimaal de volgende kolommen. De `__START_AT` en `__END_AT` worden door `APPLY CHANGES INTO` beheerd; in de `DWH_<TABEL>` view worden ze hernoemd naar de Axians-conventies.

**Op DW_-tabel zelf** (gevoed door `APPLY CHANGES INTO`):

| Kolom | Type | Betekenis |
|---|---|---|
| `WK_<TABEL>` | BIGINT (Identity) | Surrogaatsleutel, `GENERATED ALWAYS AS IDENTITY` |
| `__START_AT` | TIMESTAMP | Door Databricks beheerd — wordt `WA_FROMDATE` in view |
| `__END_AT` | TIMESTAMP | Door Databricks beheerd — wordt `WA_UNTODATE` in view |
| `WA_CRUDDTS` | TIMESTAMP | Moment van laden in DW |
| `WA_CRUD` | STRING(1) | `C`reate / `U`pdate / `D`elete (uit CDF `_change_type`) |
| `WA_SRC` | STRING | Bronsysteem |
| `WA_RUNID` | STRING | ETL run identifier |
| `WA_HASH` | STRING | Hash van alle non-BK business-kolommen (audit / reconciliation) |
| _business kolommen_ | _div._ | Alle kolommen uit YAML |
| `WK_REF_BUS_<REF>` | _als bron-BK_ | Per `foreign_keys` entry in YAML |
| `WK_REF_HASH_<REF>` | STRING | Hash van de BK van gerefereerde tabel |

**In DWH_<TABEL>-view** (bovenop DW_<TABEL>):

| Kolom | Bron | Betekenis |
|---|---|---|
| Alle kolommen van `DW_<TABEL>` | — | Doorgegeven |
| `WA_FROMDATE` | `__START_AT` | Aanvang geldigheid |
| `WA_UNTODATE` | `COALESCE(__END_AT, TIMESTAMP '9999-12-31 00:00:00')` | Einde geldigheid |
| `WA_ISCURR` | `CASE WHEN __END_AT IS NULL THEN 1 ELSE 0 END` | Huidige versie indicator |
| `WKP_<TABEL>` | `LAG(WK_<TABEL>) OVER (PARTITION BY <BK> ORDER BY __START_AT)` | Vorige versie |
| `WKR_<TABEL>` | `FIRST_VALUE(WK_<TABEL>) OVER (PARTITION BY <BK> ORDER BY __START_AT)` | Eerste versie |

### 2.6 Verplichte administratie-kolommen — Datamart

**Voor SCD Type 2 dimensies** (`DIM_<NAAM>` view):

| Kolom | Bron | Betekenis |
|---|---|---|
| `MK_<TABEL>` | `WK_<TABEL>` | Datamart sleutel (= surrogaat van DW) |
| `MK_ROOT` | `WKR_<TABEL>` | Eerste versie van entiteit (groepeert versies) |
| _business kolommen_ | — | Doorgegeven |
| `MA_FROM` | `WA_FROMDATE` | Aanvang geldigheid |
| `MA_UNTO` | `WA_UNTODATE` | Einde geldigheid |
| `MA_ISCURR` | `WA_ISCURR` | Huidige versie indicator |

**Voor SCD Type 1 dimensies** (`DIM_<NAAM>` view, alleen `WA_ISCURR = 1`):

| Kolom | Bron | Betekenis |
|---|---|---|
| `MK_<TABEL>` | `WK_<TABEL>` | Datamart sleutel |
| _business kolommen_ | — | Doorgegeven |
| `MA_CREATEDATE` | min `WA_CRUDDTS` per entiteit | Aanmaakdatum |
| `MA_CHANGEDATE` | max `WA_CRUDDTS` waar `WA_CRUD = 'U'` | Laatste wijzigingsmoment (NULL als nooit) |
| `MA_ISDEL` | `CASE WHEN WA_CRUD = 'D' THEN 1 ELSE 0 END` | Verwijderd-indicator |

**Voor feittabellen** (`FCT_<NAAM>`): geen verplichte `MA_*` kolommen, wel `MK_*` per dimensie-FK.

---

## 3. Input: YAML structuur

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
  primary: DW_SALES_LINE
  joins:                               # optioneel — extra DW_-tabellen mee-joinen
    - table: DW_SALES_ORDER
      on: order_id

dimensions:                            # FK-resolutie naar DIM_-tabellen
  - dim: DIM_KLANT
    fact_column: MK_KLANT
    join_on: klant_id                  # BK-kolom in source
    temporal_column: boekdatum         # voor SCD2: welke datum bepaalt versie
  - dim: DIM_PRODUCT
    fact_column: MK_PRODUCT
    join_on: product_id
    temporal_column: boekdatum
  - dim: DIM_DATUM
    fact_column: MK_BOEKDAT
    join_on: boekdatum                 # date-dim: geen temporal_column nodig

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

1. **Markdown header cell**: titel + verwijzing naar YAML
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

Gegenereerd vanuit `bronnen/ms_crm/klant.yml`.

**Cell 1 (markdown)**:
```markdown
# Staging — STG_KLANT
Gegenereerd vanuit `bronnen/ms_crm/klant.yml`. **Niet handmatig bewerken** — wijzig de YAML en regenereer.
```

**Cell 2 (Python — widgets)**:
```python
dbutils.widgets.text("catalog", "demo")
dbutils.widgets.text("source_system", "ms_crm")
dbutils.widgets.text("run_id", "")

catalog = dbutils.widgets.get("catalog")
source_system = dbutils.widgets.get("source_system")
run_id = dbutils.widgets.get("run_id")

spark.conf.set("c.catalog", catalog)
spark.conf.set("c.source_system", source_system)
spark.conf.set("c.run_id", run_id)
```

**Cell 3 (markdown)**:
```markdown
## STG_KLANT — streaming table met MERGE op business key
CDF staat aan zodat downstream lagen wijzigingen kunnen lezen.
```

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

> **Let op**: `APPLY CHANGES INTO ... STORED AS SCD TYPE 1` doet in Staging een upsert (huidige stand per BK). De **geschiedenis** wordt pas opgebouwd in de DW-laag, omdat alle versies tot dan via CDF beschikbaar zijn.

### 5.2 Integration — `notebooks/integration/dw_klant.ipynb`

Bevat **beide**: `DW_KLANT` (streaming table met SCD2) én `DWH_KLANT` (view).

**Cell 1 (markdown)**:
```markdown
# Integration — DW_KLANT + DWH_KLANT
Gegenereerd vanuit `bronnen/ms_crm/klant.yml`.
- `DW_KLANT`: streaming table, SCD2 via `APPLY CHANGES INTO`, surrogaat via Identity
- `DWH_KLANT`: view die Axians-conventies herstelt (WA_FROMDATE, WA_UNTODATE, WA_ISCURR, WKP_, WKR_)
```

**Cell 2 (Python — widgets)**:
```python
dbutils.widgets.text("catalog", "demo")
dbutils.widgets.text("source_system", "ms_crm")
dbutils.widgets.text("run_id", "")

catalog = dbutils.widgets.get("catalog")
source_system = dbutils.widgets.get("source_system")
run_id = dbutils.widgets.get("run_id")

spark.conf.set("c.catalog", catalog)
spark.conf.set("c.source_system", source_system)
spark.conf.set("c.run_id", run_id)
```

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

Voor `scd_type: 2` in de YAML.

**Cell 1 (markdown)**:
```markdown
# Datamart — DIM_KLANT (SCD Type 2)
View op `DWH_KLANT`. Bevat alle versies. Joins vanuit feittabellen gebeuren temporeel op `MA_FROM`/`MA_UNTO`.
```

**Cell 2 (Python — widgets)**:
```python
dbutils.widgets.text("catalog", "demo")
catalog = dbutils.widgets.get("catalog")
spark.conf.set("c.catalog", catalog)
```

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

Voor `scd_type: 1` in de YAML.

**Cell 3 (`%sql` — DIM_PRODUCT view)**:
```sql
%sql
CREATE OR REPLACE VIEW ${c.catalog}.datamart.DIM_PRODUCT AS
WITH historie AS (
  SELECT
    productcode,
    MIN(WA_CRUDDTS)                                    AS MA_CREATEDATE,
    MAX(CASE WHEN WA_CRUD = 'U' THEN WA_CRUDDTS END)   AS MA_CHANGEDATE,
    MAX(CASE WHEN WA_CRUD = 'D' THEN 1 ELSE 0 END)     AS MA_ISDEL
  FROM ${c.catalog}.integration.DWH_PRODUCT
  GROUP BY productcode
)
SELECT
  d.WK_PRODUCT  AS MK_PRODUCT,
  d.productcode,
  d.productnaam,
  h.MA_CREATEDATE,
  h.MA_CHANGEDATE,
  h.MA_ISDEL
FROM ${c.catalog}.integration.DWH_PRODUCT d
JOIN historie h ON h.productcode = d.productcode
WHERE d.WA_ISCURR = 1;
```

### 5.5 Fact tabel — `notebooks/datamart/fct_sales.ipynb`

Gegenereerd vanuit `datamart/fct_sales.yml`. Toont temporele join voor SCD2-dim en directe join voor SCD1/date-dim.

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
    -- date dimensie: directe join op datum-key
    date_format(s.boekdatum, 'yyyyMMdd')::INT AS MK_BOEKDAT,

    -- klant dimensie: temporele join (SCD Type 2)
    k.MK_KLANT,

    -- product dimensie: temporele join (SCD Type 2)
    p.MK_PRODUCT,

    -- measures
    s.regel_totaal AS OMZET,
    s.aantal       AS AANTAL,

    s.WA_CRUDDTS,
    s._commit_timestamp
  FROM STREAM table_changes('${c.catalog}.integration.DW_SALES_LINE', 0) s
  LEFT JOIN ${c.catalog}.datamart.DIM_KLANT k
    ON k.klantcode = s.klant_id
   AND s.boekdatum >= k.MA_FROM
   AND s.boekdatum <  k.MA_UNTO
  LEFT JOIN ${c.catalog}.datamart.DIM_PRODUCT p
    ON p.productcode = s.product_id
  WHERE s._change_type IN ('insert', 'update_postimage')
)
KEYS (MK_BOEKDAT, MK_KLANT, MK_PRODUCT)
SEQUENCE BY _commit_timestamp
STORED AS SCD TYPE 1;
```

> **Belangrijk**: De temporele join gebruikt `>= MA_FROM AND < MA_UNTO` (half-open interval) om dubbele matches op grensmomenten te voorkomen.

---

## 6. Regels voor Claude Code bij genereren

### 6.1 Algemeen
1. **Lees altijd eerst de relevante YAML(s)**. Verzin nooit kolommen, types, of relaties die er niet in staan.
2. **Genereer altijd .ipynb format** (geen `.sql` of `.py` losse files). Gebruik Python notebook met `%sql` magic.
3. **Overschrijf bestaande notebooks volledig** — geen merge, geen behoud van handmatige edits.
4. **Comments en markdown in het Nederlands**. Technische trefwoorden (`MERGE`, `APPLY CHANGES INTO`, `STREAMING TABLE`) blijven Engels.
5. **Serverless-compatibel blijven**: geen RDD API, geen `dbutils.fs.mount`, geen Spark-config wijzigingen die serverless niet ondersteunt.

### 6.2 Schema-naamgeving
- Staging schema: altijd `staging_<source_system>` waar `<source_system>` lowercase is van het YAML `source_system` veld.
- Integration schema: altijd `integration`.
- Datamart schema: altijd `datamart`.
- Catalog: altijd via widget — nooit hardcoden.

### 6.3 Bij iedere `CREATE STREAMING TABLE`
- Zet `delta.enableChangeDataFeed = 'true'` als de tabel downstream gelezen wordt door een andere streaming table.
- Voeg per kolom met `nullable: false` in YAML een `CONSTRAINT valid_<col> EXPECT (<col> IS NOT NULL) ON VIOLATION DROP ROW` toe.
- Zet altijd een `COMMENT` met korte beschrijving van de tabel.

### 6.4 Bij iedere `APPLY CHANGES INTO`
- `KEYS` = business_keys uit YAML.
- `SEQUENCE BY` = de `sequence_by` waarde uit YAML voor staging; `_commit_timestamp` voor integration (uit CDF van staging).
- `STORED AS SCD TYPE 1` voor staging (huidige stand per BK).
- `STORED AS SCD TYPE 2` voor integration (volledige historie).
- `COLUMNS` expliciet opsommen — gebruik nooit `*`.

### 6.5 Bij iedere DWH-view
- Voeg verplicht toe: `WA_FROMDATE`, `WA_UNTODATE`, `WA_ISCURR`, `WKP_<TABEL>`, `WKR_<TABEL>`.
- Geef alle business- en `WA_*`-kolommen door uit DW_-tabel.

### 6.6 Bij iedere DIM-view
- Lees `scd_type` uit YAML.
- Type 2: alle versies + `MK_<TABEL>`, `MK_ROOT`, `MA_FROM`, `MA_UNTO`, `MA_ISCURR`.
- Type 1: alleen `WA_ISCURR = 1` + `MK_<TABEL>`, `MA_CREATEDATE`, `MA_CHANGEDATE`, `MA_ISDEL`.

### 6.7 Bij iedere FCT-tabel
- Lees `dimensions` uit YAML. Per dimensie:
  - Met `temporal_column` → join temporeel op `MA_FROM`/`MA_UNTO` van die dimensie (SCD2 verwacht).
  - Zonder `temporal_column` → directe join op de business key (SCD1 of date-dim).
- Lees `measures` uit YAML voor de meet-kolommen.
- Lees `incremental.strategy` voor de laad-strategie.

### 6.8 Wat Claude Code NOOIT mag doen
- Notebooks met andere extensie dan `.ipynb` aanmaken.
- Catalog-namen hardcoden.
- Engelse comments toevoegen waar Nederlands moet.
- Kolommen toevoegen die niet in YAML staan (behalve de verplichte `WA_*`/`SA_*`/`MA_*`).
- `APPLY CHANGES INTO` skippen ten gunste van handgeschreven `MERGE` — dat breekt het deployment-model.
- Niet-serverless features gebruiken (RDD, mounts, init scripts, GPU clusters).
- DDL gebruiken die niet werkt in Lakeflow Declarative Pipelines.

---

## 7. Folder-structuur

```
.
├── README.md
├── databricks.yml                      # DAB config (multi-env)
├── conventies/
│   ├── naamgeving-en-lagen.md          # ← dit document
│   └── yaml-genereren-howto.md         # ← how-to voor YAML genereren
├── bronnen/                            # ← input: YAML per bron-tabel
│   ├── ms_crm/
│   │   ├── klant.yml
│   │   └── order.yml
│   └── azurestorage/
│       └── product.yml
├── datamart/                           # ← input: YAML per FCT_
│   ├── fct_sales.yml
│   └── fct_inventory.yml
└── notebooks/                          # ← output: door Claude Code gegenereerd
    ├── staging/
    │   ├── stg_klant.ipynb
    │   └── stg_order.ipynb
    ├── integration/
    │   ├── dw_klant.ipynb              # bevat DW_KLANT én DWH_KLANT
    │   └── dw_order.ipynb
    └── datamart/
        ├── dim_klant.ipynb
        ├── dim_product.ipynb
        └── fct_sales.ipynb
```

---

## 8. Snelle referentie — beslissingsboom voor Claude Code

```
Nieuwe YAML in bronnen/<bron>/<tabel>.yml?
│
├─▶ Genereer notebooks/staging/stg_<tabel>.ipynb
│   - STG_<TABEL> streaming table, MERGE op BK, CDF aan
│
├─▶ Genereer notebooks/integration/dw_<tabel>.ipynb
│   - DW_<TABEL> streaming table, SCD2 via APPLY CHANGES, Identity WK_
│   - DWH_<TABEL> view met WA_FROMDATE, WA_UNTODATE, WA_ISCURR, WKP_, WKR_
│
└─▶ datamart-blok aanwezig in YAML?
    │
    ├─▶ scd_type: 2 → notebooks/datamart/dim_<naam>.ipynb (Type 2 view)
    └─▶ scd_type: 1 → notebooks/datamart/dim_<naam>.ipynb (Type 1 view)

Nieuwe YAML in datamart/<fct_naam>.yml?
│
└─▶ Genereer notebooks/datamart/<fct_naam>.ipynb
    - FCT_<NAAM> streaming table volgens incremental strategy
    - Joins per dimensie: temporeel (met temporal_column) of direct
```
