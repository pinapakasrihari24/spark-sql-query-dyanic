# PySpark SQL Query Runner

A flexible PySpark script to execute Spark SQL queries with support for multiple queries, INSERT operations, and dynamic partitioning.

## Features

1. **Multiple Queries**: Pass multiple SQL queries separated by `~` delimiter
2. **INSERT Support**: Handles INSERT, INSERT OVERWRITE, and other DML operations
3. **Dynamic Partitioning**: Built-in configuration for dynamic partition mode (enabled by default)
4. **Flexible Input**: Accept queries via command line or from a file
5. **Verbose Logging**: Detailed execution logs with query results

## Requirements

```bash
pip install pyspark
```

## Usage

### Basic Usage

#### Single Query
```bash
spark-submit spark_sql_runner.py --query "SELECT * FROM my_table"
```

#### Multiple Queries (separated by ~)
```bash
spark-submit spark_sql_runner.py --query "SELECT * FROM table1 ~ INSERT INTO table2 SELECT * FROM table1 ~ SELECT * FROM table2"
```

#### Query from File
```bash
spark-submit spark_sql_runner.py --query-file example_queries.sql
```

### Advanced Options

#### Custom Delimiter
```bash
spark-submit spark_sql_runner.py --query "SELECT * FROM t1 ; INSERT INTO t2 SELECT * FROM t1" --delimiter ";"
```

#### Disable Dynamic Partitioning
```bash
spark-submit spark_sql_runner.py --query "INSERT INTO table VALUES (1, 'data')" --no-dynamic-partition
```

#### Custom Partition Overwrite Mode
```bash
spark-submit spark_sql_runner.py --query "INSERT OVERWRITE TABLE partitioned_table PARTITION(year) SELECT * FROM source" --partition-overwrite-mode static
```

#### Custom Application Name
```bash
spark-submit spark_sql_runner.py --query "SELECT 1" --app-name "MyDataPipeline"
```

#### Quiet Mode (Less Verbose)
```bash
spark-submit spark_sql_runner.py --query "SELECT * FROM table" --quiet
```

## Configuration

### Default Dynamic Partition Settings

The script automatically configures the following Spark settings when dynamic partitioning is enabled:

- `spark.sql.sources.partitionOverwriteMode`: `dynamic`
- `hive.exec.dynamic.partition`: `true`
- `hive.exec.dynamic.partition.mode`: `nonstrict`
- `spark.sql.adaptive.enabled`: `true`

### Modifying Configurations

You can modify the `create_spark_session()` function to add additional Spark configurations:

```python
builder = builder \
    .config("spark.sql.shuffle.partitions", "200") \
    .config("spark.default.parallelism", "100")
```

## Examples

### Example 1: Create and Query Table
```bash
spark-submit spark_sql_runner.py --query "CREATE TABLE users (id INT, name STRING) ~ INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob') ~ SELECT * FROM users"
```

### Example 2: Dynamic Partitioning
```bash
spark-submit spark_sql_runner.py --query "
INSERT OVERWRITE TABLE sales_partitioned PARTITION(year, month)
SELECT order_id, amount, product, year, month
FROM sales_staging
"
```

### Example 3: Complex Multi-Query Pipeline
```bash
spark-submit spark_sql_runner.py --query "
CREATE TABLE IF NOT EXISTS staging AS SELECT * FROM source ~
INSERT OVERWRITE TABLE target PARTITION(date_partition)
SELECT col1, col2, date_partition FROM staging WHERE status='active' ~
SELECT date_partition, COUNT(*) FROM target GROUP BY date_partition
"
```

### Example 4: Using Query File
Create a file `pipeline.sql`:
```sql
-- Step 1: Create temp table
CREATE TEMPORARY VIEW temp_sales AS
SELECT * FROM raw_sales WHERE date >= '2024-01-01'
~
-- Step 2: Insert into partitioned table
INSERT OVERWRITE TABLE processed_sales PARTITION(year, month)
SELECT order_id, amount, year, month FROM temp_sales
~
-- Step 3: Verify results
SELECT year, month, COUNT(*) as cnt, SUM(amount) as total
FROM processed_sales
GROUP BY year, month
```

Run it:
```bash
spark-submit spark_sql_runner.py --query-file pipeline.sql
```

## Command Line Arguments

| Argument | Short | Description | Default |
|----------|-------|-------------|---------|
| `--query` | `-q` | SQL query or queries (separated by delimiter) | Required* |
| `--query-file` | `-f` | Path to file containing queries | Required* |
| `--delimiter` | `-d` | Delimiter to separate queries | `~` |
| `--app-name` | | Spark application name | `SparkSQLRunner` |
| `--dynamic-partition` | | Enable dynamic partition mode | `True` |
| `--no-dynamic-partition` | | Disable dynamic partition mode | `False` |
| `--partition-overwrite-mode` | | Partition overwrite mode (dynamic/static) | `dynamic` |
| `--verbose` | `-v` | Enable verbose output | `True` |
| `--quiet` | | Disable verbose output | `False` |

*Either `--query` or `--query-file` must be provided

## Error Handling

The script includes comprehensive error handling:
- Validates that queries are provided
- Catches and reports SQL execution errors
- Properly stops Spark session on failure
- Returns appropriate exit codes

## Output

For SELECT queries, the script will:
- Display the result DataFrame
- Show row counts
- Print execution time

For INSERT/CREATE/DROP queries, the script will:
- Confirm successful execution
- Display the operation type

## Tips

1. **Large Result Sets**: For queries returning large datasets, consider using `LIMIT` or writing results to tables instead
2. **Performance**: Use `--quiet` mode for production pipelines to reduce logging overhead
3. **Debugging**: Keep `--verbose` enabled during development to see detailed execution logs
4. **Partitioning**: Always use dynamic partitioning for INSERT OVERWRITE operations on partitioned tables
5. **Query Files**: Use query files for complex multi-step pipelines for better maintainability

## Troubleshooting

### Issue: "Dynamic partition strict mode requires at least one static partition column"
**Solution**: Use `--partition-overwrite-mode dynamic` or ensure your query includes static partition columns

### Issue: Queries not splitting correctly
**Solution**: Verify your delimiter matches the one in your queries. Use `--delimiter` to specify a different delimiter

### Issue: Permission errors on Hive tables
**Solution**: Ensure proper Hive metastore configuration and user permissions

## License

This script is provided as-is for data engineering purposes.
