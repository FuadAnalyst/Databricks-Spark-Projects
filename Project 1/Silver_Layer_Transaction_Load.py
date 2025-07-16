# Databricks notebook source
# Create the table
spark.sql('''
    CREATE TABLE IF NOT EXISTS silver_layer.silver_transactions (
        transaction_id STRING,
        customer_id STRING,
        product_id STRING,
        quantity INT,
        total_amount DOUBLE,
        transaction_date DATE,
        payment_method STRING,
        store_type STRING,
        order_status STRING, -- New column
        last_updated TIMESTAMP -- New column
    )
''')

# COMMAND ----------

# get the last processed timestamp
last_processed_df = spark.sql('SELECT MAX(last_updated) as last_processed FROM silver_layer.silver_transactions')
last_processed_timestamp = last_processed_df.collect()[0]['last_processed']

# For the first run:
if last_processed_timestamp is None:
    last_processed_timestamp = '1900-01-01T00:00:00.000+00:00'

# COMMAND ----------

# Create a temporary view of incremental bronze data (the new data that is not yet in the silver layer)
spark.sql(f"""
    CREATE OR REPLACE TEMPORARY VIEW bronze_incremental_transactions AS
    SELECT *
    FROM bronze_layer.bronze_transactions tr
    WHERE tr.ingestion_timestamp > '{last_processed_timestamp}'
""")

# COMMAND ----------

display(spark.sql('SELECT * FROM bronze_incremental_transactions'))

# COMMAND ----------

# Data Transformations:

# Quantity and total_amount normalization (setting negative values to 0)
# Date casting to ensure consistent date format
# Order status derivation based on quantity and total_amount

# Data Quality Checks: We filter out records with null transaction dates, customer IDs, or product IDs.

# I will do this one using PySpark only
from pyspark.sql.functions import when, col, current_timestamp

df_new = spark.sql('SELECT * FROM bronze_incremental_transactions')
df_new = df_new.select(
    'transaction_id',
    'customer_id',
    'product_id',
    when(col('quantity') < 0, 0).otherwise(col('quantity')).alias('quantity'),
    when(col('total_amount') < 0, 0).otherwise(col('total_amount')).alias('total_amount'),
    col('transaction_date').cast('date'),
    'payment_method',
    'store_type',
    when((col('quantity') == 0) | (col('total_amount') == 0), 'Cancelled').otherwise('Completed').alias('order_status'),
    current_timestamp().alias('last_updated')
).filter(col('transaction_date').isNotNull() & col('customer_id').isNotNull() & col('product_id').isNotNull())

# COMMAND ----------

display(df_new)

# COMMAND ----------

from delta.tables import DeltaTable
df_transactions = DeltaTable.forName(spark, "silver_layer.silver_transactions")

# COMMAND ----------

df_transactions.alias("target") \
    .merge(
        df_new.alias("source"),
        "target.transaction_id = source.transaction_id"  # Join condition based on your key(s)
    ) \
    .whenMatchedUpdateAll() \
    .whenNotMatchedInsertAll() \
    .execute()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM silver_layer.silver_transactions

# COMMAND ----------

