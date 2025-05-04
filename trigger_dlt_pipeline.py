from databricks.sdk import WorkspaceClient
from databricks.sdk.service.pipelines import CreatePipeline, PipelineLibrary, PipelineNotebookLibrary

# Initialize WorkspaceClient
workspace = WorkspaceClient()

# Define your pipeline settings
pipeline_name = "Health Data DLT Pipeline"

# List all pipelines and check if our pipeline already exists
pipelines = workspace.pipelines.list()
existing_pipeline = next((p for p in pipelines if p.name == pipeline_name), None)

# If the pipeline doesn't exist, create it
if not existing_pipeline:
    # Create a notebook library configuration
    notebook_library = PipelineNotebookLibrary(
        path="/Workspace/Users/ashaik0713@gmail.com/health_dlt_pipeline"
    )
    
    # Create pipeline
    created_pipeline = workspace.pipelines.create(
        name=pipeline_name,
        development=True,  # Set to False for production
        clusters=[{
            "label": "default",
            "num_workers": 1
        }],
        libraries=[PipelineLibrary(notebook=notebook_library)],
        configuration={
            "spark.master": "local[*]",
            "spark.databricks.delta.preview.enabled": "true"
        },
        photon=True,
        edition="ADVANCED"
    )
    print(f"Created pipeline: {created_pipeline.pipeline_id}")
    pipeline_id = created_pipeline.pipeline_id
else:
    print(f"Pipeline already exists: {existing_pipeline.pipeline_id}")
    pipeline_id = existing_pipeline.pipeline_id

# Trigger the pipeline run
run = workspace.pipelines.start_update(pipeline_id=pipeline_id)
print(f"Pipeline update started. Update ID: {run.update_id}")