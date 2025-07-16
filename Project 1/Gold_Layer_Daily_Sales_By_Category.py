# Databricks notebook source
spark.sql('''
    CREATE OR REPLACE TABLE gold_layer.gold_category_sales AS
    SELECT p.category AS product_category,
           SUM(t.total_amount) AS category_total_sales
    FROM silver_layer.silver_products p
    INNER JOIN silver_layer.silver_transactions t ON p.product_id = t.product_id
    GROUP BY p.category
''')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM gold_layer.gold_category_sales

# COMMAND ----------

