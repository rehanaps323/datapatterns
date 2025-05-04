from databricks.sdk import WorkspaceClient
from databricks.sdk.service.pipelines import PipelineCluster, NotebookLibrary

# Create Databricks workspace client
w = WorkspaceClient()

# Constants
PIPELINE_NAME = "health_dlt_pipeline"
NOTEBOOK_PATH = "/Workspace/Users/your_username/health_dlt_pipeline"  # Replace with your actual notebook path

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
            NotebookLibrary(path=NOTEBOOK_PATH)  # The path to your multi-stage DLT notebook
        ],
        clusters=[
            PipelineCluster(
                label="default",
                num_workers=2,  # Increased for better performance with multiple stages
                custom_tags={
                    "pipeline_stage": "multi_stage"
                }
            )
        ],
        configuration={
            "raw_data_path": "/mnt/raw_data",
            "target": "development",
            # Data quality thresholds
            "quality.dataLossThreshold": "5.0",
            "quality.enableCatalogStatistics": "true",
            "quality.enableDataQualityDashboard": "true",
            # Performance configs
            "pipelines.autoOptimize.managed": "true",
            "pipelines.optimizeWrite": "true",
            "pipelines.trigger.interval": "1 hour",  # For production, set appropriate interval
            # Development settings
            "development_mode": "true"
        },
        continuous=False,  # Set to True for production continuous pipeline
        development=True,  # Development mode for easier debugging
        photon=True,  # Enable Photon for better performance
        channel="CURRENT"
    )
    pipeline_id = pipeline.pipeline_id
    print(f"✅ Pipeline created with ID: {pipeline_id}")

# Option 1: Trigger the pipeline run (development mode)
run = w.pipelines.start_update(
    pipeline_id=pipeline_id,
    full_refresh=True
)
print(f"🚀 Pipeline run started for pipeline ID: {pipeline_id}")

# Option 2: For production, you might want to configure scheduled runs
"""
# Configure scheduled runs (uncomment for production)
schedule = w.pipelines.set_update_schedule(
    pipeline_id=pipeline_id,
    schedule={
        "quartz_cron_expression": "0 0 */12 ? * *",  # Run every 12 hours
        "timezone_id": "UTC"
    }
)
print(f"⏰ Pipeline schedule set for pipeline ID: {pipeline_id}")
"""

# Get pipeline info and status
pipeline_info = w.pipelines.get(pipeline_id=pipeline_id)
print(f"Pipeline state: {pipeline_info.state}")
print(f"Pipeline latest update: {pipeline_info.latest_update}")

# Output pipeline URL for monitoring
workspace_url = w.config.host
print(f"📊 Monitor pipeline at: {workspace_url}/#pipeline/{pipeline_id}")