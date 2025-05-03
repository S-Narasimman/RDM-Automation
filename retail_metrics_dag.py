# Create Cloud Composer DAG
# Save the following DAG code as retail_metrics_dag.py in your Cloud Composer DAGs folder
from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.utils.dates import days_ago

default_args = {
    'start_date': days_ago(1),
    'catchup': False,
}

with DAG(
    'retail_metrics_pipeline',
    schedule_interval='@daily',
    default_args=default_args,
    description='Run RDM-Automation job on Dataproc',
) as dag:

    pyspark_job = {
        "reference": {"project_id": "your-gcp-project-id"},
        "placement": {"cluster_name": "your-dataproc-cluster"},
        "pyspark_job": {
            "main_python_file_uri": "gs://your-bucket/scripts/main.py",
            "args": []
        },
    }

    run_spark_job = DataprocSubmitJobOperator(
        task_id="run_retail_metrics_job",
        job=pyspark_job,
        region="your-region",  # e.g. "us-central1"
        project_id="your-gcp-project-id"
    )
