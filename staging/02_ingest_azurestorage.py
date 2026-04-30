# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # 02 Ingest — Azure Storage (parquet, full mode)
# MAGIC
# MAGIC **Doel:** Lees alle actieve `azurestorage`-rijen uit de control table en laad de
# MAGIC bijbehorende parquet-bestanden als Delta-tabellen in `STAGING_AZURESTORAGE`.
# MAGIC
# MAGIC **Scope:** Alleen de `full` laadstrategie is geïmplementeerd in deze slice.
# MAGIC De `incremental` (Auto Loader) variant wordt toegevoegd in Slice 2.
# MAGIC
# MAGIC **Audit-kolommen per doeltabel:**
# MAGIC
# MAGIC | Kolom | Bron |
# MAGIC |---|---|
# MAGIC | `_ingestion_timestamp` | `current_timestamp()` |
# MAGIC | `_source_system` | waarde uit control table |
# MAGIC | `_source_file` | `_metadata.file_path` |
# MAGIC | `_last_modified` | `_metadata.file_modification_time` |
# MAGIC | `_pipeline_run_id` | Workflow job run id (widget) |

# COMMAND ----------

# Parameters & widgets
dbutils.widgets.text("catalog", "DEMO_DEV", "Catalog")
dbutils.widgets.text("pipeline_run_id", "", "Pipeline Run ID (ingevuld door Workflow)")

catalog = dbutils.widgets.get("catalog")
pipeline_run_id = dbutils.widgets.get("pipeline_run_id")

print(f"Catalog          : {catalog}")
print(f"Pipeline run id  : {pipeline_run_id or '(niet opgegeven — handmatig run)'}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Stap 1 — Control table inlezen

# COMMAND ----------

control_table = f"{catalog}.CONFIG.pipeline_sources"

sources_df = spark.sql(
    f"""
    SELECT *
    FROM   {control_table}
    WHERE  source_system = 'azurestorage'
    AND    is_active     = true
    """
)

source_rows = sources_df.collect()
print(f"Actieve azurestorage-bronnen gevonden: {len(source_rows)}")
for row in source_rows:
    print(f"  → {row['target_table']} ({row['load_type']}) — {row['file_pattern']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Stap 2 — Ingest per bron

# COMMAND ----------

from pyspark.sql import functions as F

row_count_report = []

for row in source_rows:
    source_path   = row["source_path"]
    file_pattern  = row["file_pattern"]
    target_schema = row["target_schema"]
    target_table  = row["target_table"]
    load_type     = row["load_type"]
    source_system = row["source_system"]

    full_target = f"{catalog}.{target_schema}.{target_table}"
    print(f"\n--- Verwerken: {full_target} (load_type={load_type}) ---")

    # Guard: alleen 'full' is geïmplementeerd in Slice 1.
    # Slice 2 voegt de incremental / Auto Loader branch toe.
    if load_type != "full":
        raise NotImplementedError(
            f"load_type='{load_type}' is not supported in this slice. "
            "The incremental (Auto Loader) branch will be implemented in Slice 2. "
            f"Update the control table row for '{target_table}' to load_type='full' "
            "or wait for Slice 2."
        )

    # Lees alle parquet-bestanden die overeenkomen met het glob-filter.
    # includeMetadata=True geeft toegang tot _metadata.file_path en
    # _metadata.file_modification_time voor de audit-kolommen.
    raw_df = (
        spark.read.format("parquet")
        .option("pathGlobFilter", file_pattern)
        .option("recursiveFileLookup", "false")
        .load(source_path)
    )

    # Voeg de vijf audit-kolommen toe (CONTEXT.md §5).
    enriched_df = (
        raw_df
        .withColumn("_ingestion_timestamp", F.current_timestamp())
        .withColumn("_source_system",       F.lit(source_system))
        .withColumn("_source_file",         F.col("_metadata.file_path"))
        .withColumn("_last_modified",       F.col("_metadata.file_modification_time"))
        .withColumn("_pipeline_run_id",     F.lit(pipeline_run_id))
        # _metadata is een struct-kolom die niet naar Delta mag worden geschreven.
        .drop("_metadata")
    )

    # Schrijf als Delta (full = overschrijven).
    (
        enriched_df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(full_target)
    )

    written_count = spark.table(full_target).count()
    row_count_report.append((full_target, written_count))
    print(f"  Geschreven: {written_count:,} rijen → {full_target}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Stap 3 — Validatie & row counts

# COMMAND ----------

print("\n=== Row count samenvatting ===")
for table, count in row_count_report:
    print(f"  {table}: {count:,} rijen")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resultaat
# MAGIC
# MAGIC Alle actieve `azurestorage`-bronnen zijn ingeladen als Delta-tabellen in
# MAGIC `STAGING_AZURESTORAGE`. Elke tabel bevat de vijf audit-kolommen:
# MAGIC `_ingestion_timestamp`, `_source_system`, `_source_file`, `_last_modified`,
# MAGIC `_pipeline_run_id`.
# MAGIC
# MAGIC De `incremental` (Auto Loader) branch is gedefereerd naar Slice 2.
