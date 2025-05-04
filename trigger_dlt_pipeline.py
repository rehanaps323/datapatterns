from databricks.sdk import WorkspaceClient

pipeline_name = "Health Data DLT Pipeline"
#notebook_path = "/Users/your.email@databricks.com/health_dlt_pipeline"
notebook_path = "/Users/ashaik0713@gmail.com/health_dlt_pipeline"# change to your notebook path

workspace = WorkspaceClient()
pipelines = workspace.pipelines.list()

pipeline = next((p for p in pipelines if p.name == pipeline_name), None)

if not pipeline:
    created_pipeline = workspace.pipelines.create(
        name=pipeline_name,
        development=True,
        clusters=[{
            "label": "default",
            "num_workers": 1
        }],
        libraries=[{
            "notebook": {
                "path": notebook_path
            }
        }],
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

# Trigger the pipeline run
run = workspace.pipelines.start(pipeline_id, full_refresh=True)
print(f"▶️ Pipeline run started: {run.run_id}")
