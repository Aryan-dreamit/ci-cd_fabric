# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "bf217429-703b-4e28-9c78-344dcabc9068",
# META       "default_lakehouse_name": "Bronze_lakehouse",
# META       "default_lakehouse_workspace_id": "2fe9abb6-9a56-4088-8062-472540353376",
# META       "known_lakehouses": [
# META         {
# META           "id": "bf217429-703b-4e28-9c78-344dcabc9068"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# path of event house = https://onelake.dfs.fabric.microsoft.com/2fe9abb6-9a56-4088-8062-472540353376/c20a8151-c30d-419a-897b-f18e0cfc7d83/Tables/Data_for_sample/


# path of bronze Lakehouse = abfss://Test_ServicePrincipal@onelake.dfs.fabric.microsoft.com/Bronze_lakehouse.Lakehouse/Tables

# df = path

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.drop(column=["securityType"])



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
