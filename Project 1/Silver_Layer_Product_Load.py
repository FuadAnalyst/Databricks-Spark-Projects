# Databricks notebook source
# Create the table
spark.sql('''
    CREATE TABLE IF NOT EXISTS silver_layer.silver_products (
        product_id STRING,
        name STRING,
        category STRING,
        brand STRING,
        price DOUBLE,
        stock_quantity INT,
        rating DOUBLE,
        is_active BOOLEAN,
        price_category STRING,
        stock_status STRING,
        last_updated TIMESTAMP
    )
''')

# COMMAND ----------

# get the last processed timestamp
last_processed_df = spark.sql('SELECT MAX(last_updated) as last_processed FROM silver_layer.silver_products')
last_processed_timestamp = last_processed_df.collect()[0]['last_processed']

# For the first run:
if last_processed_timestamp is None:
    last_processed_timestamp = '1900-01-01T00:00:00.000+00:00'

# COMMAND ----------

# Create a temporary view of incremental bronze data (the new data that is not yet in the silver layer)
spark.sql(f"""
    CREATE OR REPLACE TEMPORARY VIEW bronze_incremental_products AS
    SELECT *
    FROM bronze_layer.bronze_products products 
    WHERE products.ingestion_timestamp > '{last_processed_timestamp}'
""")

# COMMAND ----------

display(spark.sql('SELECT * FROM bronze_incremental_products'))

# COMMAND ----------

# Data Transformations:

# Price normalization (setting negative prices to 0)
# Stock quantity normalization (setting negative stock to 0)
# Rating normalization (clamping between 0 and 5)
# Price categorization (Premium, Standard, Budget)
# Stock status calculation (Out of Stock, Low Stock, Moderate Stock, Sufficient Stock)

spark.sql('''
    CREATE OR REPLACE TEMPORARY VIEW silver_incremental_products AS
    SELECT product_id,
           name,
           brand,
           category,
           is_active,
           CASE WHEN price < 0 THEN 0 
                ELSE price 
           END AS price,
           CASE WHEN rating < 0 THEN 0 
                WHEN rating > 5 THEN 5
                ELSE rating
            END AS rating,
           CASE WHEN stock_quantity < 0 THEN 0 
                ELSE stock_quantity 
           END AS stock_quantity,
           CASE WHEN price > 1000 THEN 'Premium'
                WHEN price > 100 THEN 'Standard'
                ELSE 'Budget'
           END AS price_category,
           CASE WHEN stock_quantity = 0 THEN 'Out of Stock'
                WHEN stock_quantity < 10 THEN 'Low Stock'
                WHEN stock_quantity < 50 THEN 'Moderate Stock'
                ELSE 'Sufficient Stock'
           END AS stock_status,
           CURRENT_TIMESTAMP() AS last_updated
    FROM bronze_incremental_products
    WHERE name IS NOT NULL AND category IS NOT NULL
         
''')

# COMMAND ----------

display(spark.sql('SELECT * FROM silver_incremental_products'))

# COMMAND ----------

# If the product_id is already exists we dont want to insert that data, we will merge it (update if exists, otherwise insert)
spark.sql('''
    MERGE INTO silver_layer.silver_products target
    USING silver_incremental_products source
    ON target.product_id = source.product_id          
    WHEN MATCHED THEN
        UPDATE SET *
    WHEN NOT MATCHED THEN
        INSERT *
''')
# If we re-run again, the rows will not be inserted, only updated

# COMMAND ----------

display(spark.sql('SELECT * FROM silver_layer.silver_products'))

# COMMAND ----------

