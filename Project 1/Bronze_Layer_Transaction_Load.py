# Databricks notebook source
file_path = 'dbfs:/FileStore/Project Databricks/bronze_layer/transaction/transaction_snappy.parquet'
df = spark.read.parquet(file_path)
display(df)

# COMMAND ----------

# Change the data type of transaction_date from string to date
from pyspark.sql.functions import to_timestamp

df_cmn = df.withColumn('transaction_date', to_timestamp(df['transaction_date']))
display(df_cmn)

# COMMAND ----------

# We wanted to have an incremental facility. Whenever a new file comes in, we're going to just copy that data into our bronze layer.
# We don't want to merge or combine the data, because we want to keep all the data as it is.
# To ensure that which data is coming at what point in time, we add timestamp column.
from pyspark.sql.functions import current_timestamp

df_new = df_cmn.withColumn('ingestion_timestamp', current_timestamp())
display(df_new)

# COMMAND ----------

# Write the dataframe as delta table
df_new.write.format('delta').mode('append').saveAsTable('bronze_layer.bronze_transactions')
# Whatever the data is coming we just insert it raw

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM bronze_layer.bronze_transactions

# COMMAND ----------

display(df_new)

# COMMAND ----------

# Now, we need to remove processed file from our folder into archive folder
import datetime 

archive = 'dbfs:/FileStore/Project Databricks/bronze_layer/transaction/archive/'
archive_path = archive + '_' + datetime.datetime.now().strftime('%Y%m%d%H%M%S')
dbutils.fs.mv(file_path, archive_path)

# COMMAND ----------

