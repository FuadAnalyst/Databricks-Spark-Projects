# Databricks notebook source
file_path = 'dbfs:/FileStore/Project Databricks/bronze_layer/customer_data/customer.csv'
df = spark.read.csv(file_path, header = True, inferSchema = True)
display(df)

# COMMAND ----------

# We wanted to have an incremental facility. Whenever a new file comes in, we're going to just copy that data into our bronze layer.
# We don't want to merge or combine the data, because we want to keep all the data as it is.
# To ensure that which data is coming at what point in time, we add timestamp column.
from pyspark.sql.functions import current_timestamp

df_new = df.withColumn('ingestion_timestamp', current_timestamp())
display(df_new)

# COMMAND ----------

# Write the dataframe as delta table
df_new.write.format('delta').mode('append').saveAsTable('bronze_layer.bronze_customer')
# Whatever the data is coming we just insert it raw

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM bronze_layer.bronze_customer

# COMMAND ----------

display(df_new)

# COMMAND ----------

# Now, we need to remove processed file from our folder into archive folder
import datetime 

archive = 'dbfs:/FileStore/Project Databricks/bronze_layer/customer_data/archive/'
archive_path = archive + '_' + datetime.datetime.now().strftime('%Y%m%d%H%M%S')
dbutils.fs.mv(file_path, archive_path)

# COMMAND ----------

