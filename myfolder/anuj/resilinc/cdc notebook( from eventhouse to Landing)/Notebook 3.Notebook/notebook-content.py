# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "2ba365ca-9e83-40bb-bc32-14464a2d28d9",
# META       "default_lakehouse_name": "Landing_Lakehouse",
# META       "default_lakehouse_workspace_id": "2fe9abb6-9a56-4088-8062-472540353376",
# META       "known_lakehouses": [
# META         {
# META           "id": "2ba365ca-9e83-40bb-bc32-14464a2d28d9"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# load data using spark

# CELL ********************

df = spark.read.parquet("location to read from") 


df.write.mode("overwrite").format("delta").saveAsTable(delta_table_name)




# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# load data using pandas

# CELL ********************

import pandas as pd
df = pd.read_parquet("/lakehouse/default/Files/sample.parquet")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import SparkSession


# https://ingest-trd-qcgz6ju9a0hjmekfvu.z3.kusto.fabric.microsoft.com
# existing_table = "https://onelake.dfs.fabric.microsoft.com/2fe9abb6-9a56-4088-8062-472540353376/c20a8151-c30d-419a-897b-f18e0cfc7d83/Tables/Data_for_sample/"
existing_table = "https://ingest-trd-qcgz6ju9a0hjmekfvu.z3.kusto.fabric.microsoft.com"
updated_table = spark.read.format("delta").load(existing_table)
updated_table.show()
# display(path_of_event_house)
# df = path

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Example of query for reading data from Kusto. Replace T with your <tablename>.
kustoQuery = "['Data_for_sample'] | take 10"
# The query URI for reading the data e.g. https://<>.kusto.data.microsoft.com.
kustoUri = "https://trd-qcgz6ju9a0hjmekfvu.z3.kusto.fabric.microsoft.com"
# The database with data to be read.
database = "Sample_data"
# The access credentials.
accessToken = mssparkutils.credentials.getToken(kustoUri)
kustoDf  = spark.read\
    .format("com.microsoft.kusto.spark.synapse.datasource")\
    .option("accessToken", accessToken)\
    .option("kustoCluster", kustoUri)\
    .option("kustoDatabase", database)\
    .option("kustoQuery", kustoQuery).load()

# Example that uses the result data frame.
# kustoDf.show()

display(kustoDf)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
