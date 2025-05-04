from databricks.sdk import WorkspaceClient
from databricks.sdk.service.pipelines import PipelineCluster, NotebookLibrary

# Create the Databricks workspace client
w = WorkspaceClient()

# Create the Delta Live Table (DLT) pipeline
pipeline = w.pipelines.create(
    name="health_dlt_pipeline",
    storage="/pipelines/storage/health_dlt_pipeline",
    target="health_target",
    edition="ADVANCED",
    libraries=[
        NotebookLibrary(path="/Workspace/Users/ashaik0713@gmail.com/health_dlt_pipeline")
    ],
    clusters=[
        PipelineCluster(
            label="default",
            num_workers=1
        )
    ],
    configuration={
        "raw_data_path": "/mnt/raw_data",
        "target": "development"
    }
)

print(f"✅ Pipeline created with ID: {pipeline.pipeline_id}")
