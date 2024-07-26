CREATE EXTERNAL TABLE IF NOT EXISTS sales_info (
    transaction_id INT,
    store_id STRING,
    store_name STRING,
    location STRING,
    product_id int,
    product_name STRING,
    category STRING,
    quantity INT,
    price FLOAT,
    date STRING,
    total_amount FLOAT
)
STORED AS PARQUET
LOCATION 's3://abd-dev/fiverr/czam/target/sales_info'