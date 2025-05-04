from databricks.sdk import WorkspaceClient
from databricks.sdk.service.pipelines import PipelineCluster, NotebookLibrary

# Create Databricks workspace client
w = WorkspaceClient()

# Constants
PIPELINE_NAME = "health_dlt_pipeline"

# List all pipelines
pipelines = w.pipelines.list_pipelines()

# Find if the pipeline already exists
existing = next((p for p in pipelines if p.name == PIPELINE_NAME), None)

if existing:
    pipeline_id = existing.pipeline_id
    print(f"🔁 Pipeline already exists with ID: {pipeline_id}")
else:
    # Create the pipeline if it doesn't exist
    pipeline = w.pipelines.create(
        name=PIPELINE_NAME,
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
    pipeline_id = pipeline.pipeline_id
    print(f"✅ Pipeline created with ID: {pipeline_id}")

# Start the pipeline
w.pipelines.start_by_id(pipeline_id)
print(f"🚀 Pipeline started: {pipeline_id}")
