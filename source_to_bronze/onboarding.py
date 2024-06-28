# Databricks notebook source
df= spark.read.json(path="dbfs:/FileStore/vtex_test_schema/vtex_test_data__1_.json")

# COMMAND ----------

df.display()

# COMMAND ----------

df.write.format('delta').save("/Workspace/Users/deepa.r2019@vitstudent.ac.in/project_implementation/bronze_to_silver")

# COMMAND ----------

