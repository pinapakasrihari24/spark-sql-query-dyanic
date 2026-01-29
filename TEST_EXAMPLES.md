# Example Test Configurations for run_sql_query_dag

## Test 1: Simple SELECT Query
```json
{
  "sql_query": "SELECT 1 as test_column, 'hello' as message"
}
```

## Test 2: Create Table and Insert Data
```json
{
  "sql_query": "CREATE TABLE IF NOT EXISTS test_table (id INT, name STRING) ~ INSERT INTO test_table VALUES (1, 'Alice'), (2, 'Bob') ~ SELECT * FROM test_table"
}
```

## Test 3: Dynamic Partitioning
```json
{
  "sql_query": "CREATE TABLE IF NOT EXISTS sales (id INT, amount DOUBLE, product STRING) PARTITIONED BY (region STRING) ~ INSERT OVERWRITE TABLE sales PARTITION(region) SELECT 1, 100.0, 'Laptop', 'North' UNION ALL SELECT 2, 50.0, 'Mouse', 'South' ~ SELECT region, SUM(amount) as total FROM sales GROUP BY region",
  "partition_overwrite_mode": "dynamic",
  "enable_dynamic_partition": true
}
```

## Test 4: Multiple Queries with Custom Delimiter
```json
{
  "sql_query": "SELECT current_date() as today ; SELECT current_timestamp() as now",
  "delimiter": ";"
}
```

## Test 5: Quiet Mode
```json
{
  "sql_query": "CREATE TEMPORARY VIEW temp AS SELECT 1 as id ~ SELECT * FROM temp",
  "verbose": false,
  "app_name": "QuietTest"
}
```

## Test 6: Complex ETL Pipeline
```json
{
  "sql_query": "CREATE TEMPORARY VIEW raw_data AS SELECT 1 as order_id, 'Product A' as product, 100.0 as amount, '2024-01-15' as order_date, 'North' as region UNION ALL SELECT 2, 'Product B', 150.0, '2024-01-16', 'South' ~ CREATE TABLE IF NOT EXISTS processed_orders (order_id INT, product STRING, amount DOUBLE, order_date DATE) PARTITIONED BY (region STRING) ~ INSERT OVERWRITE TABLE processed_orders PARTITION(region) SELECT order_id, product, amount, CAST(order_date AS DATE), region FROM raw_data ~ SELECT region, COUNT(*) as order_count, SUM(amount) as total_amount FROM processed_orders GROUP BY region ORDER BY total_amount DESC",
  "app_name": "ETL_Pipeline_Test"
}
```

## Test 7: Real-world Example - Sales Report
```json
{
  "sql_query": "CREATE TEMPORARY VIEW daily_sales AS SELECT order_id, product_name, quantity, price, quantity * price as total, order_date, region FROM orders WHERE order_date >= DATE_SUB(CURRENT_DATE(), 30) ~ CREATE TABLE IF NOT EXISTS sales_summary (order_count BIGINT, total_revenue DOUBLE, avg_order_value DOUBLE) PARTITIONED BY (region STRING, report_date DATE) ~ INSERT OVERWRITE TABLE sales_summary PARTITION(region, report_date) SELECT COUNT(*) as order_count, SUM(total) as total_revenue, AVG(total) as avg_order_value, region, CURRENT_DATE() as report_date FROM daily_sales GROUP BY region ~ SELECT * FROM sales_summary WHERE report_date = CURRENT_DATE() ORDER BY total_revenue DESC",
  "partition_overwrite_mode": "dynamic",
  "verbose": true,
  "app_name": "SalesReportGenerator"
}
```

## Test 8: Data Quality Check
```json
{
  "sql_query": "CREATE TEMPORARY VIEW data_quality AS SELECT 'row_count' as metric, COUNT(*) as value FROM source_table UNION ALL SELECT 'null_count', COUNT(*) FROM source_table WHERE important_column IS NULL UNION ALL SELECT 'duplicate_count', COUNT(*) - COUNT(DISTINCT id) FROM source_table ~ SELECT * FROM data_quality",
  "app_name": "DataQualityCheck"
}
```

## Using with gcloud CLI

### Test 1
```bash
gcloud composer environments run YOUR-COMPOSER-ENV \
    --location YOUR-REGION \
    dags trigger -- \
    run_sql_query_dag \
    --conf '{"sql_query": "SELECT 1 as test_column, '\''hello'\'' as message"}'
```

### Test 2
```bash
gcloud composer environments run YOUR-COMPOSER-ENV \
    --location YOUR-REGION \
    dags trigger -- \
    run_sql_query_dag \
    --conf '{"sql_query": "CREATE TABLE IF NOT EXISTS test_table (id INT, name STRING) ~ INSERT INTO test_table VALUES (1, '\''Alice'\''), (2, '\''Bob'\'') ~ SELECT * FROM test_table"}'
```

### Test 3 (with file)
Create a file `test_config.json`:
```json
{
  "sql_query": "SELECT current_date() as today ~ SELECT current_timestamp() as now"
}
```

Then trigger:
```bash
gcloud composer environments run YOUR-COMPOSER-ENV \
    --location YOUR-REGION \
    dags trigger -- \
    run_sql_query_dag \
    --conf "$(cat test_config.json)"
```

## Using with Python

```python
from google.auth import default
from google.auth.transport.requests import Request
import requests

credentials, project = default()
credentials.refresh(Request())

AIRFLOW_URL = "https://your-airflow-url.com"
DAG_ID = "run_sql_query_dag"

test_configs = [
    {
        "sql_query": "SELECT 1 as test"
    },
    {
        "sql_query": "CREATE TABLE test (id INT) ~ INSERT INTO test VALUES (1) ~ SELECT * FROM test"
    }
]

for i, conf in enumerate(test_configs, 1):
    print(f"Running test {i}...")
    response = requests.post(
        f"{AIRFLOW_URL}/api/v1/dags/{DAG_ID}/dagRuns",
        headers={
            "Authorization": f"Bearer {credentials.token}",
            "Content-Type": "application/json"
        },
        json={"conf": conf}
    )
    print(f"Test {i} result: {response.status_code}")
    print(response.json())
```

## Validation Checklist

Before running in production, verify:

- [ ] PySpark script is uploaded to GCS
- [ ] Airflow variables are set correctly
- [ ] Dataproc cluster is running and accessible
- [ ] DAG is enabled in Airflow UI
- [ ] Test queries execute successfully
- [ ] Logs are accessible in Dataproc and Airflow
- [ ] Error handling works as expected
- [ ] Dynamic partitioning works correctly
- [ ] Performance is acceptable for your workload
