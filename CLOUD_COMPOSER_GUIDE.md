# Cloud Composer Deployment Guide
## run_sql_query_dag

This guide explains how to deploy and use the Spark SQL query DAG in Google Cloud Composer.

## Prerequisites

1. Google Cloud Composer environment (Airflow 2.x)
2. Google Cloud Dataproc cluster (permanent or ephemeral)
3. Google Cloud Storage bucket
4. Appropriate IAM permissions

## Deployment Steps

### Step 1: Upload PySpark Script to GCS

```bash
# Upload the spark_sql_runner.py script to your GCS bucket
gsutil cp spark_sql_runner.py gs://YOUR-BUCKET-NAME/scripts/

# Verify upload
gsutil ls gs://YOUR-BUCKET-NAME/scripts/
```

### Step 2: Set Airflow Variables

You can set these via the Airflow UI or using gcloud:

```bash
# Using gcloud CLI
gcloud composer environments run YOUR-COMPOSER-ENV \
    --location YOUR-REGION \
    variables set -- \
    gcp_project_id YOUR-PROJECT-ID

gcloud composer environments run YOUR-COMPOSER-ENV \
    --location YOUR-REGION \
    variables set -- \
    dataproc_region us-central1

gcloud composer environments run YOUR-COMPOSER-ENV \
    --location YOUR-REGION \
    variables set -- \
    dataproc_cluster_name your-cluster-name

gcloud composer environments run YOUR-COMPOSER-ENV \
    --location YOUR-REGION \
    variables set -- \
    gcs_bucket your-bucket-name
```

Or via Airflow UI:
- Navigate to Admin > Variables
- Add the following variables:
  - `gcp_project_id`: Your GCP project ID
  - `dataproc_region`: Region where Dataproc cluster is located
  - `dataproc_cluster_name`: Name of your Dataproc cluster
  - `gcs_bucket`: GCS bucket name (without gs:// prefix)

### Step 3: Upload DAG to Composer

```bash
# Get your Composer DAGs folder
export DAGS_FOLDER=$(gcloud composer environments describe YOUR-COMPOSER-ENV \
    --location YOUR-REGION \
    --format="get(config.dagGcsPrefix)")

# Upload the DAG file
# For permanent cluster version:
gsutil cp run_sql_query_dag_permanent_cluster.py ${DAGS_FOLDER}/run_sql_query_dag.py

# OR for ephemeral cluster version:
gsutil cp run_sql_query_dag.py ${DAGS_FOLDER}/run_sql_query_dag.py
```

### Step 4: Verify DAG in Airflow UI

1. Navigate to your Cloud Composer environment's Airflow UI
2. Look for `run_sql_query_dag` in the DAGs list
3. Toggle the DAG to ON state

## Usage

### Method 1: Trigger via Airflow UI

1. Go to the Airflow UI
2. Find `run_sql_query_dag`
3. Click on the "Trigger DAG" button (play icon)
4. In the configuration JSON, provide:

```json
{
  "sql_query": "SELECT * FROM my_table ~ INSERT INTO target_table SELECT * FROM source_table"
}
```

Optional parameters:
```json
{
  "sql_query": "SELECT * FROM my_table",
  "delimiter": "~",
  "app_name": "MySparkJob",
  "partition_overwrite_mode": "dynamic",
  "enable_dynamic_partition": true,
  "verbose": true
}
```

### Method 2: Trigger via gcloud CLI

```bash
# Basic trigger with single query
gcloud composer environments run YOUR-COMPOSER-ENV \
    --location YOUR-REGION \
    dags trigger -- \
    run_sql_query_dag \
    --conf '{"sql_query": "SELECT * FROM my_table"}'

# Multiple queries
gcloud composer environments run YOUR-COMPOSER-ENV \
    --location YOUR-REGION \
    dags trigger -- \
    run_sql_query_dag \
    --conf '{"sql_query": "CREATE TABLE test (id INT) ~ INSERT INTO test VALUES (1) ~ SELECT * FROM test"}'

# With additional parameters
gcloud composer environments run YOUR-COMPOSER-ENV \
    --location YOUR-REGION \
    dags trigger -- \
    run_sql_query_dag \
    --conf '{"sql_query": "INSERT OVERWRITE TABLE partitioned_table PARTITION(year, month) SELECT * FROM source", "partition_overwrite_mode": "dynamic", "verbose": false}'
```

### Method 3: Trigger via Airflow REST API

```bash
# Get your Airflow webserver URL
AIRFLOW_URL=$(gcloud composer environments describe YOUR-COMPOSER-ENV \
    --location YOUR-REGION \
    --format="get(config.airflowUri)")

# Trigger DAG
curl -X POST \
    "${AIRFLOW_URL}/api/v1/dags/run_sql_query_dag/dagRuns" \
    -H "Content-Type: application/json" \
    -H "Authorization: Bearer $(gcloud auth print-access-token)" \
    -d '{
        "conf": {
            "sql_query": "SELECT * FROM my_table"
        }
    }'
```

### Method 4: Trigger via Python Client

```python
from google.auth import default
from google.auth.transport.requests import Request
import requests
import json

# Get credentials
credentials, project = default()
credentials.refresh(Request())

# Composer environment details
AIRFLOW_URL = "https://your-airflow-url.com"
DAG_ID = "run_sql_query_dag"

# Trigger DAG
url = f"{AIRFLOW_URL}/api/v1/dags/{DAG_ID}/dagRuns"
headers = {
    "Authorization": f"Bearer {credentials.token}",
    "Content-Type": "application/json"
}

conf = {
    "sql_query": "SELECT * FROM table1 ~ INSERT INTO table2 SELECT * FROM table1"
}

response = requests.post(
    url,
    headers=headers,
    json={"conf": conf}
)

print(response.json())
```

## Configuration Parameters

### Required Parameters

| Parameter | Type | Description | Example |
|-----------|------|-------------|---------|
| `sql_query` | string | SQL query or queries (separated by delimiter) | `"SELECT * FROM table1"` |

### Optional Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `delimiter` | string | `~` | Character to separate multiple queries |
| `app_name` | string | `SparkSQLRunner` | Spark application name |
| `partition_overwrite_mode` | string | `dynamic` | Partition overwrite mode (`dynamic` or `static`) |
| `enable_dynamic_partition` | boolean | `true` | Enable dynamic partitioning |
| `verbose` | boolean | `true` | Enable verbose logging |

## Examples

### Example 1: Simple SELECT Query
```json
{
  "sql_query": "SELECT * FROM sales WHERE date >= '2024-01-01'"
}
```

### Example 2: Multiple Queries
```json
{
  "sql_query": "CREATE TEMPORARY VIEW temp_data AS SELECT * FROM source ~ INSERT OVERWRITE TABLE target PARTITION(date) SELECT * FROM temp_data ~ SELECT COUNT(*) FROM target"
}
```

### Example 3: Dynamic Partitioning with INSERT
```json
{
  "sql_query": "INSERT OVERWRITE TABLE sales_partitioned PARTITION(year, month, day) SELECT order_id, amount, product, year, month, day FROM sales_staging WHERE status = 'completed'",
  "partition_overwrite_mode": "dynamic",
  "enable_dynamic_partition": true
}
```

### Example 4: Custom Delimiter
```json
{
  "sql_query": "SELECT * FROM table1 ; INSERT INTO table2 SELECT * FROM table1",
  "delimiter": ";"
}
```

### Example 5: Quiet Mode for Production
```json
{
  "sql_query": "INSERT INTO production_table SELECT * FROM staging_table",
  "verbose": false,
  "app_name": "ProductionETL"
}
```

## Monitoring

### View DAG Logs

1. In Airflow UI, click on the DAG run
2. Click on the task `submit_spark_sql_query`
3. Click "Log" to view execution logs

### View Dataproc Job Logs

```bash
# List recent jobs
gcloud dataproc jobs list \
    --region=YOUR-REGION \
    --cluster=YOUR-CLUSTER-NAME

# Get job details
gcloud dataproc jobs describe JOB-ID \
    --region=YOUR-REGION

# View job driver logs
gcloud dataproc jobs wait JOB-ID \
    --region=YOUR-REGION
```

## Troubleshooting

### Issue: "sql_query parameter is required"
**Solution**: Ensure you pass the `sql_query` in the DAG configuration when triggering

### Issue: Cluster not found
**Solution**: Verify the cluster name in Airflow Variables matches your actual cluster name

### Issue: Script not found in GCS
**Solution**: Verify the script path and ensure it's uploaded to the correct GCS location

### Issue: Permission denied
**Solution**: Ensure the Composer service account has the following roles:
- Dataproc Editor
- Storage Object Viewer (for GCS bucket)
- Compute Viewer

### Issue: Dynamic partition errors
**Solution**: Ensure Hive metastore is configured and the table is partitioned

## Best Practices

1. **Use Permanent Clusters**: For frequent jobs, use permanent clusters to avoid startup time
2. **Parameterize Queries**: Use templating in your queries for flexibility
3. **Monitor Costs**: Track Dataproc cluster usage and optimize cluster size
4. **Error Handling**: Enable email alerts for failed DAG runs
5. **Version Control**: Keep DAGs in version control (Git)
6. **Testing**: Test queries in development environment before production
7. **Security**: Use Secret Manager for sensitive data, not plain text in queries

## Cost Optimization

1. Use ephemeral clusters for infrequent jobs
2. Use preemptible workers for non-critical workloads
3. Auto-scale cluster based on workload
4. Use appropriate machine types for your workload

## Security Considerations

1. Use IAM roles and service accounts properly
2. Encrypt data at rest and in transit
3. Use VPC Service Controls if needed
4. Audit access and operations
5. Never hardcode credentials in DAGs

## Additional Resources

- [Cloud Composer Documentation](https://cloud.google.com/composer/docs)
- [Dataproc Documentation](https://cloud.google.com/dataproc/docs)
- [Apache Airflow Documentation](https://airflow.apache.org/docs/)
