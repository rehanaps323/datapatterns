from databricks.sdk import WorkspaceClient
from databricks.sdk.service.pipelines import CreatePipelineCluster, PipelineLibrary, NotebookLibrary

pipeline_name = "Health Data DLT Pipeline"
notebook_path = "/Users/ashaik0713@gmail.com/health_dlt_pipeline"

workspace = WorkspaceClient()

# Convert generator to list
pipelines = list(workspace.pipelines.list_pipelines())

# Check if pipeline already exists
pipeline = next((p for p in pipelines if p.name == pipeline_name), None)

# Create pipeline if it doesn't exist
if not pipeline:
    created_pipeline = workspace.pipelines.create(
        name=pipeline_name,
        development=True,
        clusters=[
            CreatePipelineCluster(
                label="default",
                num_workers=1
            )
        ],
        libraries=[
            PipelineLibrary(
                notebook=NotebookLibrary(
                    path=notebook_path
                )
            )
        ],
        configuration={
            "spark.master": "local[*]",
            "spark.databricks.delta.preview.enabled": "true"
        },
        photon=True,
        edition="ADVANCED"
    )
    print(f"✅ Created pipeline: {created_pipeline.pipeline_id}")
    pipeline_id = created_pipeline.pipeline_id
else:
    print(f"ℹ️ Pipeline already exists: {pipeline.pipeline_id}")
    pipeline_id = pipeline.pipeline_id

# Start pipeline run
run = workspace.pipelines.start(pipeline_id, full_refresh=True)
print(f"▶️ Pipeline run started. Run ID: {run.run_id}")
