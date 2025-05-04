from databricks.sdk import WorkspaceClient
from databricks.sdk.service.pipelines import CreatePipeline, PipelineCluster, NotebookLibrary
from databricks.sdk.service.jobs import SubmitRun

# Create Databricks workspace client
w = WorkspaceClient()

# Constants
PIPELINE_NAME = "health_dlt_pipeline"

# List all pipelines
pipelines = w.pipelines.list()

# Find if the pipeline already exists
existing = next((p for p in pipelines if p.name == PIPELINE_NAME), None)

if existing:
    pipeline_id = existing.pipeline_id
    print(f"🔁 Pipeline already exists with ID: {pipeline_id}")
else:
    # Create the pipeline if it doesn't exist
    pipeline = w.pipelines.create(
        create=CreatePipeline(
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
    )
    pipeline_id = pipeline.pipeline_id
    print(f"✅ Pipeline created with ID: {pipeline_id}")

# Trigger the pipeline run
run = w.jobs.submit_run(
    run_name="health_dlt_pipeline_run",
    pipeline_id=pipeline_id  # For DLT pipelines, use pipeline_id instead of job_id
)
print(f"🚀 Pipeline run started for pipeline ID: {pipeline_id}, run ID: {run.run_id}")