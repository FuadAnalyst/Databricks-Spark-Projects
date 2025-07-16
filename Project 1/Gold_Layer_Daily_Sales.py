# Databricks notebook source
spark.sql('''
    CREATE OR REPLACE TABLE gold_layer.gold_daily_sales AS
    SELECT transaction_date,
           SUM(total_amount) AS daily_total_sales
    FROM silver_layer.silver_transactions
    GROUP BY transaction_date
''')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM gold_layer.gold_daily_sales

# COMMAND ----------

