# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "cbbe1b55-5d0b-41fe-9d05-8f2b422aab79",
# META       "default_lakehouse_name": "bronze_layer",
# META       "default_lakehouse_workspace_id": "2fe9abb6-9a56-4088-8062-472540353376",
# META       "known_lakehouses": [
# META         {
# META           "id": "cbbe1b55-5d0b-41fe-9d05-8f2b422aab79"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql import SparkSession
import pandas as pd
import re

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # **Resource List 1**

# CELL ********************


df = spark.read.format("csv").option("header", True).load("abfss://Test_ServicePrincipal@onelake.dfs.fabric.microsoft.com/bronze_layer.Lakehouse/Files/Resource List 1 (1).csv")


new_cols = [re.sub(r'[^a-zA-Z0-9_]', '', c.lower().replace(' ', '_').replace('.', '_').replace('(', '').replace(')', ''))
            for c in df.columns]

for old, new in zip(df.columns, new_cols):
    df = df.withColumnRenamed(old, new)



df.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable("Resource_list")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # **Contract Search**

# CELL ********************

df = spark.read.format("csv").option("header", True).load("abfss://Test_ServicePrincipal@onelake.dfs.fabric.microsoft.com/bronze_layer.Lakehouse/Files/Contract Search Results 1 (1).csv")


new_cols = [re.sub(r'[^a-zA-Z0-9_]', '', c.lower().replace(' ', '_').replace('.', '_').replace('(', '').replace(')', ''))
            for c in df.columns]
for old, new in zip(df.columns, new_cols):
    df = df.withColumnRenamed(old, new)
df.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable("contract_search")

display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


excel_file_path = 'abfss://Test_ServicePrincipal@onelake.dfs.fabric.microsoft.com/bronze_layer.Lakehouse/Files/Billing & Customer Hours by Contract_Combo Billing Item Detail & Customer Hour by Contract 1 (1).xlsx'
df = pd.read_excel(excel_file_path,header=2)

new_cols = [re.sub(r'[^a-zA-Z0-9_]', '', c.lower().replace(' ', '_').replace('.', '_').replace('(', '').replace(')', ''))
            for c in df.columns]
df.columns = new_cols 
df_spark = spark.createDataFrame(df)

df_spark.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable("Billing_Customer")

display(df_spark)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # **Digital transformation**

# CELL ********************

excel_file_path = 'abfss://Test_ServicePrincipal@onelake.dfs.fabric.microsoft.com/bronze_layer.Lakehouse/Files/Digital Transformation Hours (1).xlsx'
df = pd.read_excel(excel_file_path)

new_cols = [re.sub(r'[^a-zA-Z0-9_]', '', c.lower().replace(' ', '_').replace('.', '_').replace('(', '').replace(')', ''))
            for c in df.columns]
df.columns = new_cols 
df_spark = spark.createDataFrame(df)

df_spark.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable("Digital_transformation")

display(df_spark)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # **Ireland Holiday**

# CELL ********************

excel_file_path = 'abfss://Test_ServicePrincipal@onelake.dfs.fabric.microsoft.com/bronze_layer.Lakehouse/Files/Ireland Holiday 2024 - 2027 2 (1).xlsx'
df = pd.read_excel(excel_file_path)

new_cols = [re.sub(r'[^a-zA-Z0-9_]', '', c.lower().replace(' ', '_').replace('.', '_').replace('(', '').replace(')', ''))
            for c in df.columns]
df.columns = new_cols 
df_spark = spark.createDataFrame(df)

df_spark.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable("Ireland_holiday")

display(df_spark)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
