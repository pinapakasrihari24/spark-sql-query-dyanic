"""
Airflow DAG for running Spark SQL queries in Cloud Composer (Permanent Cluster Version)
DAG Name: run_sql_query_dag
This version assumes you have a permanent Dataproc cluster
"""

from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable
from datetime import timedelta


# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG Configuration
dag = DAG(
    'run_sql_query_dag',
    default_args=default_args,
    description='Execute Spark SQL queries on Dataproc',
    schedule_interval=None,  # Triggered manually or via API
    start_date=days_ago(1),
    catchup=False,
    tags=['spark', 'sql', 'dataproc'],
)

# Configuration Variables (set these in Airflow Variables)
PROJECT_ID = Variable.get("gcp_project_id", default_var="your-project-id")
REGION = Variable.get("dataproc_region", default_var="us-central1")
CLUSTER_NAME = Variable.get("dataproc_cluster_name", default_var="permanent-spark-cluster")
BUCKET_NAME = Variable.get("gcs_bucket", default_var="your-bucket-name")

# PySpark script location in GCS
PYSPARK_SCRIPT_GCS = f"gs://{BUCKET_NAME}/scripts/spark_sql_runner.py"


def build_pyspark_job(**context):
    """
    Build the PySpark job configuration
    Gets sql_query from dag_run.conf
    """
    # Get the sql_query from dag_run configuration
    dag_run_conf = context.get('dag_run').conf if context.get('dag_run') else {}
    sql_query = dag_run_conf.get('sql_query', '')
    
    if not sql_query:
        raise ValueError("sql_query parameter is required in dag_run configuration")
    
    # Additional optional parameters
    delimiter = dag_run_conf.get('delimiter', '~')
    app_name = dag_run_conf.get('app_name', 'SparkSQLRunner')
    partition_overwrite_mode = dag_run_conf.get('partition_overwrite_mode', 'dynamic')
    enable_dynamic_partition = dag_run_conf.get('enable_dynamic_partition', True)
    verbose = dag_run_conf.get('verbose', True)
    
    # Build arguments list
    args = [
        '--query', sql_query,
        '--delimiter', delimiter,
        '--app-name', app_name,
        '--partition-overwrite-mode', partition_overwrite_mode
    ]
    
    if not enable_dynamic_partition:
        args.append('--no-dynamic-partition')
    
    if not verbose:
        args.append('--quiet')
    
    return {
        "reference": {"project_id": PROJECT_ID},
        "placement": {"cluster_name": CLUSTER_NAME},
        "pyspark_job": {
            "main_python_file_uri": PYSPARK_SCRIPT_GCS,
            "args": args,
            "properties": {
                "spark.sql.sources.partitionOverwriteMode": partition_overwrite_mode,
                "hive.exec.dynamic.partition": "true",
                "hive.exec.dynamic.partition.mode": "nonstrict",
            }
        }
    }


# Submit Spark Job Task
submit_pyspark_job = DataprocSubmitJobOperator(
    task_id='submit_spark_sql_query',
    job=build_pyspark_job,
    region=REGION,
    project_id=PROJECT_ID,
    dag=dag,
)

# Simple workflow - just submit the job
submit_pyspark_job
