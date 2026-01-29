"""
Airflow DAG for running Spark SQL queries in Cloud Composer
DAG Name: run_sql_query_dag
"""

from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocSubmitJobOperator,
    DataprocCreateClusterOperator,
    DataprocDeleteClusterOperator
)
from airflow.utils.dates import days_ago
from airflow.models import Variable
from datetime import timedelta
import json


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

# Configuration Variables (can be set in Airflow Variables or passed as dag_run conf)
PROJECT_ID = Variable.get("gcp_project_id", default_var="your-project-id")
REGION = Variable.get("dataproc_region", default_var="us-central1")
CLUSTER_NAME = Variable.get("dataproc_cluster_name", default_var="ephemeral-spark-cluster-{{ ds_nodash }}-{{ ts_nodash }}")
BUCKET_NAME = Variable.get("gcs_bucket", default_var="your-bucket-name")

# PySpark script location in GCS
PYSPARK_SCRIPT_GCS = f"gs://{BUCKET_NAME}/scripts/spark_sql_runner.py"


def get_dataproc_cluster_config():
    """
    Returns Dataproc cluster configuration
    Can be customized based on workload requirements
    """
    return {
        "master_config": {
            "num_instances": 1,
            "machine_type_uri": "n1-standard-4",
            "disk_config": {
                "boot_disk_type": "pd-standard",
                "boot_disk_size_gb": 100
            }
        },
        "worker_config": {
            "num_instances": 2,
            "machine_type_uri": "n1-standard-4",
            "disk_config": {
                "boot_disk_type": "pd-standard",
                "boot_disk_size_gb": 100
            }
        },
        "software_config": {
            "image_version": "2.1-debian11",
            "properties": {
                "spark:spark.sql.sources.partitionOverwriteMode": "dynamic",
                "hive:hive.exec.dynamic.partition": "true",
                "hive:hive.exec.dynamic.partition.mode": "nonstrict",
                "spark:spark.sql.adaptive.enabled": "true"
            }
        }
    }


# Task 1: Create Dataproc Cluster (optional - use if you want ephemeral clusters)
create_cluster = DataprocCreateClusterOperator(
    task_id='create_dataproc_cluster',
    project_id=PROJECT_ID,
    cluster_config=get_dataproc_cluster_config(),
    region=REGION,
    cluster_name=CLUSTER_NAME,
    dag=dag,
)


# Task 2: Submit Spark Job
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


submit_pyspark_job = DataprocSubmitJobOperator(
    task_id='submit_spark_sql_query',
    job=build_pyspark_job,
    region=REGION,
    project_id=PROJECT_ID,
    dag=dag,
)


# Task 3: Delete Dataproc Cluster (optional - use if you created ephemeral cluster)
delete_cluster = DataprocDeleteClusterOperator(
    task_id='delete_dataproc_cluster',
    project_id=PROJECT_ID,
    cluster_name=CLUSTER_NAME,
    region=REGION,
    trigger_rule='all_done',  # Delete cluster even if job fails
    dag=dag,
)


# Set task dependencies
# Option 1: With ephemeral cluster (create -> submit -> delete)
create_cluster >> submit_pyspark_job >> delete_cluster

# Option 2: Without cluster management (just submit to existing cluster)
# Uncomment this and comment out the line above if using a permanent cluster
# submit_pyspark_job
