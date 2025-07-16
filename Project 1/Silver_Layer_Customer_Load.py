# Databricks notebook source
# MAGIC %md
# MAGIC When the new data comes we will merge them

# COMMAND ----------

# Create the table
spark.sql('''
    CREATE TABLE IF NOT EXISTS silver_layer.silver_customers (
        customer_id STRING,
        name STRING,
        email STRING,
        country STRING,
        customer_type STRING,
        registration_date DATE,
        age INT,
        gender STRING,
        total_purchases INT,
        customer_segment STRING, -- New column
        days_since_registration INT, -- New column
        last_updated TIMESTAMP -- New column
    )
''')

# COMMAND ----------

# get the last processed timestamp
last_processed_df = spark.sql('SELECT MAX(last_updated) as last_processed FROM silver_layer.silver_customers')
last_processed_timestamp = last_processed_df.collect()[0]['last_processed']

# For the first run:
if last_processed_timestamp is None:
    last_processed_timestamp = '1900-01-01T00:00:00.000+00:00'

# COMMAND ----------

# Create a temporary view of incremental bronze data (the new data that is not yet in the silver layer)
spark.sql(f"""
    CREATE OR REPLACE TEMPORARY VIEW bronze_incremental AS
    SELECT *
    FROM bronze_layer.bronze_customer c 
    WHERE c.ingestion_timestamp > '{last_processed_timestamp}'
""")

# COMMAND ----------

display(spark.sql('SELECT * FROM bronze_incremental'))

# COMMAND ----------

# Data Transformations:

# Validate email addresses (null or not null)
# Valid age between 18 to 100
# Create customer_segment as total_purchases > 10000 THEN 'High Value' if > 5000 THEN 'Medium Value' ELSE 'Low Value'
# days since user is registered in the system
# Remove any junk records where total_purchase is negative number

spark.sql('''
    CREATE OR REPLACE TEMPORARY VIEW silver_incremental AS
    SELECT customer_id,
    name, 
    email,
    country,
    customer_type,
    registration_date,
    age,
    gender,
    total_purchases,
    CASE WHEN total_purchases > 10000 THEN 'High Value' 
         WHEN total_purchases > 5000 THEN 'Medium Value'
         ELSE 'Low Value' 
    END AS customer_segment,
    DATEDIFF(CURRENT_DATE(), registration_date) AS days_since_registration,
    CURRENT_TIMESTAMP() AS last_updated
    FROM bronze_incremental
    WHERE email IS NOT NULL AND age BETWEEN 18 AND 100 AND total_purchases >= 0
''')

# COMMAND ----------

display(spark.sql('SELECT * FROM silver_incremental'))

# COMMAND ----------

# If the customer_id is already exists we dont want to insert that data, we will merge it (update if exists, otherwise insert)
spark.sql('''
    MERGE INTO silver_layer.silver_customers target
    USING silver_incremental source
    ON target.customer_id = source.customer_id          
    WHEN MATCHED THEN
        UPDATE SET *
    WHEN NOT MATCHED THEN
        INSERT *
''')
# If we re-run again, the rows will not be inserted, only updated

# COMMAND ----------

display(spark.sql('SELECT * FROM silver_layer.silver_customers'))

# COMMAND ----------

