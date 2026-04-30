# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # 00 Bootstrap
# MAGIC
# MAGIC **Doel:** Eenmalige (maar idempotente) aanmaak van:
# MAGIC - De Unity Catalog `DEMO_DEV`
# MAGIC - De drie schema's: `STAGING_AZURESTORAGE`, `STAGING_SQLSERVER`, `CONFIG`
# MAGIC - Het External Volume op de Azure Storage External Location
# MAGIC - De control table `DEMO_DEV.CONFIG.pipeline_sources` met twee seed-rijen
# MAGIC
# MAGIC Dit notebook is volledig idempotent: meerdere keren draaien geeft hetzelfde resultaat
# MAGIC en leidt niet tot fouten of dubbele rijen.

# COMMAND ----------

# Parameters & widgets
dbutils.widgets.text("catalog", "DEMO_DEV", "Catalog")

catalog = dbutils.widgets.get("catalog")

# Storage URL voor het parquet-bron-volume. Hardcoded omdat deze slice op één
# dev workspace draait; latere slices (5 — test/prod stubs) parameteriseren dit.
AZURE_STORAGE_URL = "abfss://sourcetables@tastybytessa.dfs.core.windows.net/"

print(f"Catalog            : {catalog}")
print(f"Azure Storage URL  : {AZURE_STORAGE_URL}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Stap 1 — Catalog aanmaken

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
spark.sql(f"USE CATALOG {catalog}")
print(f"Catalog '{catalog}' bestaat of is aangemaakt.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Stap 2 — Schema's aanmaken

# COMMAND ----------

for schema in ["STAGING_AZURESTORAGE", "STAGING_SQLSERVER", "CONFIG"]:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
    print(f"Schema '{catalog}.{schema}' bestaat of is aangemaakt.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Stap 3 — External Volume aanmaken
# MAGIC
# MAGIC Het volume wijst naar de `source/` sub-map van de Azure Storage container die wordt
# MAGIC beheerd door de opgegeven External Location.

# COMMAND ----------

volume_sql = f"""
CREATE EXTERNAL VOLUME IF NOT EXISTS {catalog}.STAGING_AZURESTORAGE.source
  LOCATION '{AZURE_STORAGE_URL}'
"""

spark.sql(volume_sql)
print(
    f"External Volume '{catalog}.STAGING_AZURESTORAGE.source' bestaat of is aangemaakt."
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Stap 4 — Control table aanmaken

# COMMAND ----------

create_control_table_sql = f"""
CREATE TABLE IF NOT EXISTS {catalog}.CONFIG.pipeline_sources (
  source_system  STRING  NOT NULL COMMENT 'Bronsysteem (bijv. azurestorage)',
  source_path    STRING  NOT NULL COMMENT 'Pad naar de bronfolder (volume-pad)',
  file_pattern   STRING  NOT NULL COMMENT 'Glob filter binnen de folder (per doeltabel)',
  target_schema  STRING  NOT NULL COMMENT 'Doelschema binnen de catalog',
  target_table   STRING  NOT NULL COMMENT 'Doeltabelnaam',
  file_format    STRING  NOT NULL COMMENT 'Bestandstype (bijv. parquet)',
  is_active      BOOLEAN NOT NULL COMMENT 'Aan/uit zonder rij te verwijderen',
  load_type      STRING  NOT NULL COMMENT 'full of incremental (Auto Loader)'
)
USING DELTA
COMMENT 'Control table — stuurt de parquet-ingest pipeline aan'
"""

spark.sql(create_control_table_sql)
print(f"Control table '{catalog}.CONFIG.pipeline_sources' bestaat of is aangemaakt.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Stap 5 — Seed-rijen laden (MERGE — idempotent)

# COMMAND ----------

source_path = f"/Volumes/{catalog.lower()}/staging_azurestorage/source"

merge_sql = f"""
MERGE INTO {catalog}.CONFIG.pipeline_sources AS tgt
USING (
  SELECT
    'azurestorage'                                      AS source_system,
    '{source_path}'                                     AS source_path,
    'ORDER_HEADER_*.parquet'                            AS file_pattern,
    'STAGING_AZURESTORAGE'                              AS target_schema,
    'order_header'                                      AS target_table,
    'parquet'                                           AS file_format,
    true                                                AS is_active,
    'full'                                              AS load_type
  UNION ALL
  SELECT
    'azurestorage'                                      AS source_system,
    '{source_path}'                                     AS source_path,
    'ORDER_DETAIL_*.parquet'                            AS file_pattern,
    'STAGING_AZURESTORAGE'                              AS target_schema,
    'order_detail'                                      AS target_table,
    'parquet'                                           AS file_format,
    true                                                AS is_active,
    'full'                                              AS load_type
) AS src
ON  tgt.source_system = src.source_system
AND tgt.target_table  = src.target_table
WHEN NOT MATCHED THEN INSERT *
"""

spark.sql(merge_sql)
print("Seed-rijen geladen (MERGE — geen duplicaten bij herdraaien).")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Stap 6 — Validatie & row counts

# COMMAND ----------

result = spark.sql(
    f"SELECT * FROM {catalog}.CONFIG.pipeline_sources ORDER BY target_table"
)
print(f"\nAantal rijen in control table: {result.count()}")
result.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resultaat
# MAGIC
# MAGIC Bootstrap voltooid. De volgende objecten zijn aangemaakt (of bestonden al):
# MAGIC
# MAGIC | Object | Pad |
# MAGIC |--------|-----|
# MAGIC | Catalog | `DEMO_DEV` |
# MAGIC | Schema | `DEMO_DEV.STAGING_AZURESTORAGE` |
# MAGIC | Schema | `DEMO_DEV.STAGING_SQLSERVER` |
# MAGIC | Schema | `DEMO_DEV.CONFIG` |
# MAGIC | External Volume | `DEMO_DEV.STAGING_AZURESTORAGE.source` |
# MAGIC | Control table | `DEMO_DEV.CONFIG.pipeline_sources` (2 rijen) |
