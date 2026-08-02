# Databricks notebook source
# MAGIC %md
# MAGIC # Health Data Processing Pipeline
# MAGIC 
# MAGIC This Delta Live Tables pipeline implements a medallion architecture:
# MAGIC - Bronze: Raw ingestion layer
# MAGIC - Silver: Validated and cleaned data
# MAGIC - Gold: Business-level aggregates

# COMMAND ----------

import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, DoubleType
import datetime

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# Define the schema for CSV file
patient_schema = StructType([
    StructField("patient_id", StringType(), False),
    StructField("age", IntegerType(), True),
    StructField("gender", StringType(), True),
    StructField("blood_type", StringType(), True),
    StructField("diagnosis", StringType(), True),
    StructField("treatment_date", DateType(), True),
    StructField("heart_rate", IntegerType(), True),
    StructField("blood_pressure", StringType(), True),
    StructField("temperature", DoubleType(), True),
    StructField("weight", DoubleType(), True)
])

# Source file location (set in the pipeline configuration)
source_path = spark.conf.get("raw_data_path", "/mnt/raw_data")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze Layer - Raw Data Ingestion

# COMMAND ----------

@dlt.table(
    name="bronze_patients",
    comment="Raw patient data from CSV files with minimal processing"
)
def bronze_patients():
    # Read CSV files from the specified location
    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("header", "true")
            .schema(patient_schema)
            .load(f"{source_path}/patients/*.csv")
            # Add metadata columns
            .withColumn("_ingest_timestamp", F.current_timestamp())
            .withColumn("_source_file", F.input_file_name())
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Layer - Validated and Cleaned Data

# COMMAND ----------

# Define data quality expectations
@dlt.table(
    name="silver_patients",
    comment="Validated and cleaned patient data"
)
@dlt.expect_all_or_drop({
    "valid_patient_id": "patient_id IS NOT NULL AND length(patient_id) > 0",
    "valid_age": "age IS NULL OR (age > 0 AND age < 120)",
    "valid_gender": "gender IS NULL OR gender IN ('M', 'F', 'Other')",
    "valid_blood_type": "blood_type IS NULL OR blood_type IN ('A+', 'A-', 'B+', 'B-', 'AB+', 'AB-', 'O+', 'O-')",
    "valid_heart_rate": "heart_rate IS NULL OR (heart_rate > 30 AND heart_rate < 220)",
    "valid_temperature": "temperature IS NULL OR (temperature > 35.0 AND temperature < 42.0)",
})
def silver_patients():
    # Read from bronze and apply transformations
    return (
        dlt.read_stream("bronze_patients")
            # Clean and standardize data
            .withColumn("gender", F.upper(F.trim(F.col("gender"))))
            .withColumn("blood_type", F.upper(F.trim(F.col("blood_type"))))
            # Parse blood pressure into systolic and diastolic
            .withColumn("systolic_bp", 
                F.when(F.col("blood_pressure").isNotNull(), 
                       F.split(F.col("blood_pressure"), "/").getItem(0).cast("int"))
                .otherwise(None))
            .withColumn("diastolic_bp", 
                F.when(F.col("blood_pressure").isNotNull(), 
                       F.split(F.col("blood_pressure"), "/").getItem(1).cast("int"))
                .otherwise(None))
            # Add data quality column
            .withColumn("is_valid_bp", 
                F.col("systolic_bp").isNotNull() & 
                F.col("diastolic_bp").isNotNull() & 
                (F.col("systolic_bp") > F.col("diastolic_bp")) &
                (F.col("systolic_bp") >= 80) & 
                (F.col("systolic_bp") <= 200) &
                (F.col("diastolic_bp") >= 40) & 
                (F.col("diastolic_bp") <= 120))
            # Add processing timestamp
            .withColumn("_processed_timestamp", F.current_timestamp())
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Layer - Business Level Aggregations

# COMMAND ----------

@dlt.table(
    name="gold_diagnosis_summary",
    comment="Patient diagnosis summary statistics"
)
def gold_diagnosis_summary():
    return (
        dlt.read("silver_patients")
            .groupBy("diagnosis")
            .agg(
                F.count("patient_id").alias("patient_count"),
                F.round(F.avg("age"), 2).alias("avg_age"),
                F.round(F.stddev("age"), 2).alias("stddev_age"),
                F.countDistinct("gender").alias("gender_diversity"),
                F.round(F.avg("heart_rate"), 2).alias("avg_heart_rate"),
                F.round(F.avg("systolic_bp"), 2).alias("avg_systolic_bp"),
                F.round(F.avg("diastolic_bp"), 2).alias("avg_diastolic_bp"),
                F.round(F.avg("temperature"), 2).alias("avg_temperature"),
                F.round(F.avg("weight"), 2).alias("avg_weight"),
                # Calculate % of valid blood pressure readings
                F.round(F.avg(F.when(F.col("is_valid_bp") == True, 1).otherwise(0)) * 100, 2)
                    .alias("valid_bp_percentage")
            )
            .withColumn("last_updated", F.current_timestamp())
    )

# COMMAND ----------

@dlt.table(
    name="gold_daily_metrics",
    comment="Daily health metrics aggregation"
)
def gold_daily_metrics():
    return (
        dlt.read("silver_patients")
            .withColumn("treatment_date", F.coalesce(F.col("treatment_date"), F.to_date(F.col("_ingest_timestamp"))))
            .groupBy(F.col("treatment_date").alias("date"))
            .agg(
                F.count("patient_id").alias("daily_patient_count"),
                F.countDistinct("patient_id").alias("unique_patient_count"),
                F.round(F.avg("heart_rate"), 2).alias("avg_heart_rate"),
                F.round(F.avg("systolic_bp"), 2).alias("avg_systolic_bp"),
                F.round(F.avg("diastolic_bp"), 2).alias("avg_diastolic_bp"),
                F.round(F.avg("temperature"), 2).alias("avg_temperature"),
                # Distribution of diagnoses
                F.collect_set(F.struct("diagnosis", F.count("diagnosis").over(
                    F.partitionBy("treatment_date", "diagnosis")).alias("count"))).alias("diagnoses")
            )
            .orderBy(F.col("date").desc())
            .withColumn("last_updated", F.current_timestamp())
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Live Testing Table

# COMMAND ----------

@dlt.table(
    name="data_quality_metrics",
    comment="Data quality metrics for monitoring"
)
def data_quality_metrics():
    """Calculate data quality metrics for monitoring"""
    bronze_df = dlt.read("bronze_patients")
    silver_df = dlt.read("silver_patients")
    
    # Get total counts
    bronze_count = bronze_df.count()
    silver_count = silver_df.count()
    
    # Build quality metrics dataframe
    metrics = []
    
    # Add timestamp
    current_time = datetime.datetime.now()
    
    # Basic volume metrics
    metrics.append(("bronze_record_count", bronze_count, current_time))
    metrics.append(("silver_record_count", silver_count, current_time))
    metrics.append(("data_loss_percentage", 
                   100 - (silver_count / bronze_count * 100) if bronze_count > 0 else 0, 
                   current_time))
    
    # Add null metrics for silver layer
    for column in silver_df.columns:
        null_count = silver_df.filter(F.col(column).isNull()).count()
        null_percentage = (null_count / silver_count * 100) if silver_count > 0 else 0
        metrics.append((f"silver_{column}_null_percentage", null_percentage, current_time))
    
    # Convert to DataFrame
    metrics_df = spark.createDataFrame(metrics, ["metric_name", "metric_value", "calculation_time"])
    
    return metrics_df
