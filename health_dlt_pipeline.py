from dlt import table, read, read_stream, expect_all_or_drop, enable_local_execution
enable_local_execution()



from pyspark.sql.functions import col, split, current_timestamp, input_file_name, isnan, count
import json
import os
from databricks.sdk import WorkspaceClient
#from databricks.sdk.service.pipelines import PipelineMode

# ---------------- BRONZE TABLE ---------------- #
@dlt.table(
    name="bronze_patients",
    comment="Raw health data ingested from CSV into bronze layer."
)
def bronze_patients():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load("/Volumes/your_volume_path/Heart Prediction Quantum Dataset.csv")
        .withColumn("_ingest_timestamp", current_timestamp())
        .withColumn("_source_file", input_file_name())
    )

# ---------------- SILVER TABLE ---------------- #
@dlt.table(
    name="silver_patients",
    comment="Cleaned and transformed health data with derived fields."
)
@dlt.expect_all_or_drop({
    "non_null_patient_id": "patient_id IS NOT NULL",
    "valid_age": "age > 0 AND age < 120",
    "valid_heart_rate": "heart_rate > 0",
})
def silver_patients():
    df = dlt.read_stream("bronze_patients")
    df = df.withColumn("blood_pressure_systolic", split(col("blood_pressure"), "/").getItem(0).cast("int"))
    df = df.withColumn("blood_pressure_diastolic", split(col("blood_pressure"), "/").getItem(1).cast("int"))
    df = df.withColumn("is_blood_pressure_normal", (col("blood_pressure_systolic") < 120) & (col("blood_pressure_diastolic") < 80))
    return df.drop("blood_pressure")

# ---------------- GOLD TABLES ---------------- #
@dlt.table(
    name="gold_diagnosis_summary",
    comment="Aggregated metrics grouped by diagnosis."
)
def gold_diagnosis_summary():
    df = dlt.read("silver_patients")
    return (
        df.groupBy("diagnosis")
        .agg(
            {"age": "avg", "heart_rate": "avg", "*": "count"}
        )
        .withColumnRenamed("avg(age)", "average_age")
        .withColumnRenamed("avg(heart_rate)", "average_heart_rate")
        .withColumnRenamed("count(1)", "patient_count")
    )

@dlt.table(
    name="gold_daily_metrics",
    comment="Daily treatment metrics."
)
def gold_daily_metrics():
    df = dlt.read("silver_patients")
    return (
        df.groupBy("treatment_date")
        .agg(
            {"heart_rate": "avg", "*": "count"}
        )
        .withColumnRenamed("avg(heart_rate)", "average_heart_rate")
        .withColumnRenamed("count(1)", "treatment_count")
    )

# ---------------- DATA QUALITY METRICS ---------------- #
@dlt.table(
    name="data_quality_metrics",
    comment="Metrics on data quality and ingestion."
)
def data_quality_metrics():
    bronze_df = dlt.read("bronze_patients")
    silver_df = dlt.read("silver_patients")
    record_count = bronze_df.count()
    null_heart_rate = bronze_df.filter(col("heart_rate").isNull()).count()
    null_rate = null_heart_rate / record_count if record_count else None
    silver_count = silver_df.count()
    loss_rate = 1 - (silver_count / record_count) if record_count else None

    return spark.createDataFrame([{
        "bronze_record_count": record_count,
        "silver_record_count": silver_count,
        "heart_rate_null_rate": null_rate,
        "silver_loss_rate": loss_rate
    }])

# ---------------- PIPELINE DEPLOYMENT ---------------- #
workspace = WorkspaceClient()

pipeline_name = "Health Data DLT Pipeline"

existing_pipelines = workspace.pipelines.list()
pipeline = next((p for p in existing_pipelines if p.name == pipeline_name), None)

if not pipeline:
    created_pipeline = workspace.pipelines.create(
        name=pipeline_name,
        development=True,
        clusters=[{
            "label": "default",
            "num_workers": 1
        }],
        libraries=[{
            "notebook": {
                "path": "/Users/ashaik0713@gmail.com/health_dlt_pipeline"
            }
        }],
        configuration={
            "spark.master": "local[*]",
            "spark.databricks.delta.preview.enabled": "true"
        },
        photon=True,
        edition="ADVANCED"
    )
    print(f"Created pipeline: {created_pipeline.pipeline_id}")
else:
    created_pipeline = pipeline
    print(f"Pipeline already exists: {created_pipeline.pipeline_id}")

# Trigger the pipeline run
run = workspace.pipelines.start(created_pipeline.pipeline_id, full_refresh=True)
print(f"Pipeline run started. Run ID: {run.run_id}")
