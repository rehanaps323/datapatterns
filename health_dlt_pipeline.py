from databricks.sdk import WorkspaceClient
from databricks.sdk.service.pipelines import CreatePipelineRequest, PipelineCluster, NotebookLibrary

# Create a Workspace client
w = WorkspaceClient()

pipeline = w.pipelines.create(
    name="simple_demo_pipeline",
    storage="/pipelines/storage/simple_demo_pipeline",
    target="simple_demo_target",
    notebooks=[
        NotebookLibrary(path="/Workspace/Users/ashaik0713@gmail.com/health_dlt_pipeline")
    ],
    edition="ADVANCED",
    clusters=[
        PipelineCluster(
            label="default",
            num_workers=1  # Simple cluster config if autoscale isn't exposed
        )
    ],
    configuration={
        "raw_data_path": "/mnt/raw_data",
        "target": "development"
    }
)

print(f"Pipeline created with ID: {pipeline.pipeline_id}")
