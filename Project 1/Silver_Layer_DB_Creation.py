# Databricks notebook source
spark.sql('CREATE DATABASE IF NOT EXISTS silver_layer')

# COMMAND ----------

display(spark.sql('SHOW DATABASES'))

# COMMAND ----------

