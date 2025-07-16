# Databricks notebook source
# MAGIC %md
# MAGIC ![Screenshot](/Workspace/Users/fuadgusejnov90@gmail.com/Screenshot 2025-07-09 162903.png)

# COMMAND ----------

# MAGIC %md
# MAGIC **Key Components**
# MAGIC 1. Databricks Workspace
# MAGIC 2. Databricks File System (DBFS)
# MAGIC 3. Databricks Clusters
# MAGIC 4. Databricks Notebooks
# MAGIC 5. Delta Lake
# MAGIC 6. Databricks Jobs
# MAGIC 7. PowerBI

# COMMAND ----------

# Create Bronze layer database
spark.sql('CREATE DATABASE IF NOT EXISTS bronze_layer')

# COMMAND ----------

display(spark.sql('SHOW DATABASES'))

# COMMAND ----------

spark.sql('USE bronze_layer')

# COMMAND ----------

display(spark.sql('SELECT current_database()'))

# COMMAND ----------

