# Databricks notebook source
from pyspark.sql.functions import col, datediff, count, lit, sum, avg, when
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %sql
# MAGIC use vtex_db

# COMMAND ----------

# Create a DataFrame from the Delta table registered in the Hive metastore
orders = spark.read.format("delta").table("vtex_db.items_df_final")

# Show the DataFrame
# orders.display()

result_df = orders.groupBy("order_id").count()
print(result_df.count())

# COMMAND ----------

