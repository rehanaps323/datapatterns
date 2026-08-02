from databricks.sdk import WorkspaceClient
import json

# Initialize WorkspaceClient
workspace = WorkspaceClient()

# Define your pipeline settings
pipeline_name = "Health Data DLT Pipeline"

# Check if pipeline exists by listing all pipelines
try:
    # Use the get method to list all pipelines
    all_pipelines = workspace.api_client.do('GET', '/pipelines')
    
    # Find if our pipeline exists
    existing_pipeline = None
    for pipeline in all_pipelines.get('statuses', []):
        if pipeline.get('name') == pipeline_name:
            existing_pipeline = pipeline
            break
            
    pipeline_id = None
    
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
        
        # Create pipeline using direct API call
        response = workspace.api_client.do('POST', '/pipelines', data=json.dumps(pipeline_spec))
        pipeline_id = response.get('pipeline_id')
        print(f"Created pipeline: {pipeline_id}")
    else:
        pipeline_id = existing_pipeline.get('pipeline_id')
        print(f"Pipeline already exists: {pipeline_id}")

    # Trigger the pipeline run
    if pipeline_id:
        update_spec = {"full_refresh": True}
        update_response = workspace.api_client.do('POST', f'/pipelines/{pipeline_id}/updates', 
                                                 data=json.dumps(update_spec))
        update_id = update_response.get('update_id')
        print(f"Pipeline update started. Update ID: {update_id}")
    else:
        print("Error: No pipeline ID available. Pipeline creation may have failed.")
        
except Exception as e:
    print(f"Error: {str(e)}")
    # Fallback to direct REST API using workspace.api_client methods
    print("Check your SDK version and try updating it with: pip install --upgrade databricks-sdk")