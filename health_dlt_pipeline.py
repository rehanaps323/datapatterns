
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.pipelines import AutoScale, PipelineCluster, NotebookLibrary


# Create a workspace client using env vars or ~/.databrickscfg
w = WorkspaceClient()

# Define the pipeline settings
pipeline = w.pipelines.create(
    name="simple_demo_pipeline",
    storage="/pipelines/storage/simple_demo_pipeline",  # DBFS or volume path
    target="simple_demo_target",  # Target schema
    notebooks=[
        NotebookLibrary(path="/Workspace/Users/ashaik0713@gmail.com/health_dlt_pipeline/dlt_demo")  # Path to your notebook
    ],
    edition="ADVANCED",  # Use "CORE" if you don't have advanced features
    clusters=[
        PipelineCluster(
            label="default",
            autoscale=AutoScale(min_workers=1, max_workers=2),
            channel="CURRENT"  # Optional: CURRENT, PREVIEW, etc.
        )
    ],
    configuration={
        "raw_data_path": "/mnt/raw_data",
        "target": "development"
    }
)

print(f"Pipeline created with ID: {pipeline.pipeline_id}")

# # Create the DLT pipeline for this project using the custom Databricks Academy class DAPipelineConfig that was created using the Databricks SDK. 

# pipeline = DAPipelineConfig(pipeline_name=f"sdk_health_etl_{DA.catalog_dev}", 
#                             catalog=f"{DA.catalog_dev}",
#                             schema="default", 
#                             pipeline_notebooks=[
#                                 "/src/dlt_pipelines/ingest-bronze-silver_dlt", 
#                                 "/src/dlt_pipelines/gold_tables_dlt",
#                                 "/tests/integration_test/integration_tests_dlt"
#                               ],
#                             config_variables={
#                                 'target':'development', 
#                                 'raw_data_path': f'/Volumes/{DA.catalog_dev}/default/health'
#                               }
#                           )

# pipeline.create_dlt_pipeline()

# pipeline.start_dlt_pipeline()

# #after the pipeline is created, verify th pipeline in the data bricks UI 
# #now, we are going to use this pipeline and build .csv to bronze
# import dlt
# import pyspark.sql.functions as F

# ## Add previous folder to python path to import our helpers package
# import sys
# sys.path.append('../.')
# from helpers import project_functions


# ## Store the target configuration environment in the variable targert
# target = spark.conf.get("target")

# ## Store the target raw data configuration in the variable raw_data_path
# raw_data_path = spark.conf.get("raw_data_path")

# ## The health_bronze table is created using the value based on the target variable.
# ## development - import the DEV CSV
# ## stage - import the STAGE CSV
# ## production - import the daily CSV files from our production source volume


# ## Simple expectations for the bronze table
# valid_rows = {
#         "not_null_pii": "PII IS NOT NULL", 
#         "valid_date": "date IS NOT NULL"
#     }

# @dlt.table(
#     comment = "This table will be used to ingest the raw CSV files and add metadata columns to the bronze table.",
#     table_properties = {"quality": "bronze"}
# )

# ## Fail process if expectation is not met
# @dlt.expect_all_or_fail(valid_rows)

# def health_bronze():
#     return (
#         spark
#         .readStream
#         .format("cloudFiles")
#         .option("cloudFiles.format", "csv")
#         .option("header","true")
#         .schema(project_functions.get_health_csv_schema())   ## Use the custom schema we created
#         .load(raw_data_path)   ## <--------------- Path is based on the configuration parameter set (DEV, STAGE, PROD)
#         .select(
#             "*",
#             "_metadata.file_name",
#             "_metadata.file_modification_time",
#             F.current_timestamp().alias("processing_time")
#             )
#     )