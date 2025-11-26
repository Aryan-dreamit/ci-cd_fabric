# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "c82cf3dd-7102-40d5-b484-0d0db5470c20",
# META       "default_lakehouse_name": "gold_layer",
# META       "default_lakehouse_workspace_id": "2fe9abb6-9a56-4088-8062-472540353376",
# META       "known_lakehouses": [
# META         {
# META           "id": "c82cf3dd-7102-40d5-b484-0d0db5470c20"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

df=spark.read.format("delta").load("abfss://Test_ServicePrincipal@onelake.dfs.fabric.microsoft.com/silver_layer.Lakehouse/Tables/billing_customer")
df.write.format("delta").mode("overwrite").option("overwriteschema","true").saveAsTable("billing_customer")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df=spark.read.format("delta").load("abfss://Test_ServicePrincipal@onelake.dfs.fabric.microsoft.com/silver_layer.Lakehouse/Tables/contract_search")
df.write.format("delta").mode("overwrite").option("overwriteschema","true").saveAsTable("contract_search")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df=spark.read.format("delta").load("abfss://Test_ServicePrincipal@onelake.dfs.fabric.microsoft.com/silver_layer.Lakehouse/Tables/digital_transformation")
df.write.format("delta").mode("overwrite").option("overwriteschema","true").saveAsTable("digital_transformation")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df=spark.read.format("delta").load("abfss://Test_ServicePrincipal@onelake.dfs.fabric.microsoft.com/silver_layer.Lakehouse/Tables/ireland_holiday")
df.write.format("delta").mode("overwrite").option("overwriteschema","true").saveAsTable("ireland_holiday")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df=spark.read.format("delta").load("abfss://Test_ServicePrincipal@onelake.dfs.fabric.microsoft.com/silver_layer.Lakehouse/Tables/resource_list")
df.write.format("delta").mode("overwrite").option("overwriteschema","true").saveAsTable("resource_list")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import SparkSession
import pandas as pd
import re
excel_file_path = 'abfss://Test_ServicePrincipal@onelake.dfs.fabric.microsoft.com/gold_layer.Lakehouse/Files/Billing & Customer Hours by Contract_Combo Billing Item Detail & Customer Hour by Contract 1 (1).xlsx'
df = pd.read_excel(excel_file_path)

new_cols = [re.sub(r'[^a-zA-Z0-9_]', '', c.lower().replace(' ', '_').replace('.', '_').replace('(', '').replace(')', ''))
            for c in df.columns]
df.columns = new_cols  
df_spark = spark.createDataFrame(df)

df_spark.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable("Billing & Customer Hours by Contract_Combo (2) ")

display(df_spark)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df=spark.read

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
