-- Example SQL queries file
-- Queries are separated by ~ delimiter

-- Query 1: Create a sample table
CREATE TABLE IF NOT EXISTS sales_data (
    order_id INT,
    product_name STRING,
    amount DOUBLE,
    order_date DATE,
    region STRING
)
~
-- Query 2: Insert sample data
INSERT INTO sales_data VALUES
(1, 'Laptop', 999.99, '2024-01-15', 'North'),
(2, 'Mouse', 29.99, '2024-01-16', 'South'),
(3, 'Keyboard', 79.99, '2024-01-17', 'East')
~
-- Query 3: Query the data
SELECT * FROM sales_data
~
-- Query 4: Create partitioned table
CREATE TABLE IF NOT EXISTS sales_partitioned (
    order_id INT,
    product_name STRING,
    amount DOUBLE,
    order_date DATE
)
PARTITIONED BY (region STRING)
~
-- Query 5: Insert into partitioned table with dynamic partitioning
INSERT OVERWRITE TABLE sales_partitioned PARTITION(region)
SELECT order_id, product_name, amount, order_date, region
FROM sales_data
~
-- Query 6: Query partitioned table
SELECT region, COUNT(*) as order_count, SUM(amount) as total_sales
FROM sales_partitioned
GROUP BY region
