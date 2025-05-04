from databricks.sdk import WorkspaceClient
import json

# Initialize WorkspaceClient
workspace = WorkspaceClient()

# Define your pipeline settings
pipeline_name = "Health Data DLT Pipeline"

# List all pipelines and check if our pipeline already exists
pipelines = workspace.pipelines.list()
existing_pipeline = next((p for p in pipelines if p.name == pipeline_name), None)

# If the pipeline doesn't exist, create it
if not existing_pipeline:
    # Create pipeline specification as a dictionary that matches the API structure
    pipeline_spec = {
        "name": pipeline_name,
        "development": True,  # Set to False for production
        "clusters": [
            {
                "label": "default",
                "num_workers": 1
            }
        ],
        "libraries": [
            {
                "notebook": {
                    "path": "/Workspace/Users/ashaik0713@gmail.com/health_dlt_pipeline"
                }
            }
        ],
        "configuration": {
            "spark.master": "local[*]",
            "spark.databricks.delta.preview.enabled": "true"
        },
        "photon": True,
        "edition": "ADVANCED"
    }
    
    # Create pipeline (using the method that works with your SDK version)
    created_pipeline = workspace.post("pipelines", json=pipeline_spec)
    pipeline_id = created_pipeline.get("pipeline_id")
    print(f"Created pipeline: {pipeline_id}")
else:
    pipeline_id = existing_pipeline.pipeline_id
    print(f"Pipeline already exists: {pipeline_id}")

# Trigger the pipeline run (using the method that works with your SDK version)
update_response = workspace.post(f"pipelines/{pipeline_id}/updates", json={"full_refresh": True})
update_id = update_response.get("update_id")
print(f"Pipeline update started. Update ID: {update_id}")