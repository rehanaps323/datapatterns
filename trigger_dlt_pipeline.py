from databricks.sdk import WorkspaceClient
from databricks.sdk.service.pipelines import Pipeline, PipelineSettings, RunPipeline

# Initialize WorkspaceClient
workspace = WorkspaceClient()

# Define your pipeline settings
pipeline_name = "Health Data DLT Pipeline"

# Create or retrieve an existing pipeline
pipelines = workspace.pipelines.list()  # List all pipelines
existing_pipeline = next((p for p in pipelines if p.name == pipeline_name), None)

# If the pipeline doesn't exist, create it
if not existing_pipeline:
    pipeline_settings = PipelineSettings(
        name=pipeline_name,
        development=True,  # Set to False for production
        clusters=[{
            "label": "default",
            "num_workers": 1
        }],
        libraries=[{
            "notebook": {
                "path": "/Workspace/Users/ashaik0713@gmail.com/health_dlt_pipeline"
            }
        }],
        configuration={
            "spark.master": "local[*]",
            "spark.databricks.delta.preview.enabled": "true"
        },
        photon=True,
        edition="ADVANCED"
    )
    
    # Create pipeline
    created_pipeline = workspace.pipelines.create(pipeline_settings)
    print(f"Created pipeline: {created_pipeline.pipeline_id}")
else:
    print(f"Pipeline already exists: {existing_pipeline.pipeline_id}")

# Trigger the pipeline run
run = workspace.pipelines.start(existing_pipeline.pipeline_id if existing_pipeline else created_pipeline.pipeline_id, full_refresh=True)
print(f"Pipeline run started. Run ID: {run.run_id}")
