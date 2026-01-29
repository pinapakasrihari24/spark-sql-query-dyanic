#!/usr/bin/env python3
"""
PySpark SQL Query Runner
Executes Spark SQL queries with support for multiple queries and dynamic partitioning
"""

import argparse
import sys
from pyspark.sql import SparkSession


def create_spark_session(app_name="SparkSQLRunner", enable_dynamic_partition=True):
    """
    Create and configure Spark session with dynamic partition settings
    
    Args:
        app_name: Name of the Spark application
        enable_dynamic_partition: Whether to enable dynamic partitioning
    
    Returns:
        SparkSession object
    """
    builder = SparkSession.builder.appName(app_name)
    
    # Default dynamic partition configurations
    if enable_dynamic_partition:
        builder = builder \
            .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
            .config("hive.exec.dynamic.partition", "true") \
            .config("hive.exec.dynamic.partition.mode", "nonstrict") \
            .config("spark.sql.adaptive.enabled", "true")
    
    return builder.enableHiveSupport().getOrCreate()


def execute_queries(spark, queries, verbose=True):
    """
    Execute one or more SQL queries
    
    Args:
        spark: SparkSession object
        queries: List of SQL queries to execute
        verbose: Whether to print execution details
    
    Returns:
        List of DataFrames for SELECT queries, None for others
    """
    results = []
    
    for idx, query in enumerate(queries, 1):
        query = query.strip()
        if not query:
            continue
            
        if verbose:
            print(f"\n{'='*80}")
            print(f"Executing Query {idx}/{len(queries)}:")
            print(f"{'-'*80}")
            print(query)
            print(f"{'='*80}\n")
        
        try:
            # Execute the query
            result_df = spark.sql(query)
            
            # Check if it's a SELECT query (returns data)
            query_type = query.strip().upper().split()[0]
            
            if query_type == "SELECT":
                if verbose:
                    print(f"Query {idx} completed successfully. Showing results:")
                    result_df.show(truncate=False)
                    print(f"\nRow count: {result_df.count()}")
                results.append(result_df)
            else:
                # For INSERT, CREATE, DROP, etc.
                if verbose:
                    print(f"Query {idx} ({query_type}) executed successfully.")
                results.append(None)
                
        except Exception as e:
            print(f"\nERROR executing Query {idx}:")
            print(f"Error: {str(e)}\n")
            raise
    
    return results


def parse_arguments():
    """
    Parse command line arguments
    
    Returns:
        Parsed arguments
    """
    parser = argparse.ArgumentParser(
        description="Execute Spark SQL queries with dynamic partition support",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Single query
  python spark_sql_runner.py --query "SELECT * FROM table1"
  
  # Multiple queries separated by ~
  python spark_sql_runner.py --query "SELECT * FROM table1 ~ INSERT INTO table2 SELECT * FROM table1"
  
  # With custom dynamic partition config
  python spark_sql_runner.py --query "INSERT OVERWRITE TABLE partitioned_table PARTITION(year, month) SELECT * FROM source" --dynamic-partition
  
  # Read query from file
  python spark_sql_runner.py --query-file queries.sql
        """
    )
    
    query_group = parser.add_mutually_exclusive_group(required=True)
    query_group.add_argument(
        '--query', '-q',
        type=str,
        help='SQL query or queries separated by ~ delimiter'
    )
    query_group.add_argument(
        '--query-file', '-f',
        type=str,
        help='Path to file containing SQL queries (separated by ~)'
    )
    
    parser.add_argument(
        '--delimiter', '-d',
        type=str,
        default='~',
        help='Delimiter to separate multiple queries (default: ~)'
    )
    
    parser.add_argument(
        '--app-name',
        type=str,
        default='SparkSQLRunner',
        help='Spark application name (default: SparkSQLRunner)'
    )
    
    parser.add_argument(
        '--dynamic-partition',
        action='store_true',
        default=True,
        help='Enable dynamic partition mode (default: True)'
    )
    
    parser.add_argument(
        '--no-dynamic-partition',
        action='store_true',
        help='Disable dynamic partition mode'
    )
    
    parser.add_argument(
        '--partition-overwrite-mode',
        type=str,
        choices=['dynamic', 'static'],
        default='dynamic',
        help='Partition overwrite mode (default: dynamic)'
    )
    
    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        default=True,
        help='Enable verbose output (default: True)'
    )
    
    parser.add_argument(
        '--quiet',
        action='store_true',
        help='Disable verbose output'
    )
    
    return parser.parse_args()


def main():
    """
    Main execution function
    """
    args = parse_arguments()
    
    # Determine verbose mode
    verbose = args.verbose and not args.quiet
    
    # Read queries
    if args.query:
        query_string = args.query
    else:
        with open(args.query_file, 'r') as f:
            query_string = f.read()
    
    # Split queries by delimiter
    queries = [q.strip() for q in query_string.split(args.delimiter) if q.strip()]
    
    if not queries:
        print("ERROR: No valid queries provided")
        sys.exit(1)
    
    if verbose:
        print(f"Found {len(queries)} query/queries to execute")
    
    # Determine dynamic partition setting
    enable_dynamic_partition = args.dynamic_partition and not args.no_dynamic_partition
    
    # Create Spark session
    spark = create_spark_session(
        app_name=args.app_name,
        enable_dynamic_partition=enable_dynamic_partition
    )
    
    # Set partition overwrite mode
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", args.partition_overwrite_mode)
    
    if verbose:
        print(f"\nSpark Session Configuration:")
        print(f"  - App Name: {args.app_name}")
        print(f"  - Dynamic Partition: {enable_dynamic_partition}")
        print(f"  - Partition Overwrite Mode: {args.partition_overwrite_mode}")
    
    try:
        # Execute queries
        results = execute_queries(spark, queries, verbose)
        
        if verbose:
            print(f"\n{'='*80}")
            print(f"All queries executed successfully!")
            print(f"{'='*80}\n")
        
    except Exception as e:
        print(f"\nFATAL ERROR: {str(e)}")
        spark.stop()
        sys.exit(1)
    
    finally:
        spark.stop()
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
