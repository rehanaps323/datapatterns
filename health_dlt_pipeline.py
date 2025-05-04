from databricks.sdk import WorkspaceClient
from databricks.sdk.service.pipelines import PipelineCluster, NotebookLibrary

# Create the Databricks workspace client (uses env vars DATABRICKS_HOST and DATABRICKS_TOKEN)
w = WorkspaceClient()

# Define the Delta Live Table (DLT) pipeline
pipeline = w.pipelines.create(
    name="health_dlt_pipeline",
    storage="/pipelines/storage/health_dlt_pipeline",  # Must be DBFS or Unity Catalog volume path
    target="health_target",  # Will create or use this schema
    notebooks=[
        NotebookLibrary(path="/Workspace/Users/ashaik0713@gmail.com/health_dlt_pipeline")
    ],
    edition="ADVANCED",
    clusters=[
        PipelineCluster(
            label="default",
            num_workers=1  # Minimal cluster
        )
    ],
    configuration={
        "raw_data_path": "/mnt/raw_data",
        "target": "development"
    }
)

print(f"✅ Pipeline created with ID: {pipeline.pipeline_id}")
