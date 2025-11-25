# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "e8d4a3cf-987f-49a4-80c0-995473dc8732",
# META       "default_lakehouse_name": "aryan",
# META       "default_lakehouse_workspace_id": "2fe9abb6-9a56-4088-8062-472540353376",
# META       "known_lakehouses": [
# META         {
# META           "id": "e8d4a3cf-987f-49a4-80c0-995473dc8732"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!

df = spark.read.format("csv").load("abfss://Test_ServicePrincipal@onelake.dfs.fabric.microsoft.com/aryan.Lakehouse/Tables/dbo/customer_subscriptions")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
