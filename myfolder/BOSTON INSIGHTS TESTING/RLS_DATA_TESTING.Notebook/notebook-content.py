# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "e16edb7f-0c8d-4efd-a220-208dac031ea5",
# META       "default_lakehouse_name": "RLS_LAKEHOUSE",
# META       "default_lakehouse_workspace_id": "2fe9abb6-9a56-4088-8062-472540353376",
# META       "known_lakehouses": [
# META         {
# META           "id": "e6107edf-cd6b-4426-b49b-7473f6354076"
# META         },
# META         {
# META           "id": "e16edb7f-0c8d-4efd-a220-208dac031ea5"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# import pyspark
# import pandas as pd 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# pd=pd.read.format("delta").load("bronze_layer.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("""
    CREATE TABLE IF NOT EXISTS users
    USING DELTA
    LOCATION 'Tables/users'
""")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("""
    ALTER TABLE users
    ADD COLUMNS (
        name STRING,
        email STRING
    )
""")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC INSERT INTO users (name, email)
# MAGIC VALUES
# MAGIC     ('Aryan', 'Aryan@dreamitcs.com'),
# MAGIC     ('Shivam', 'Shivam@dreamitcs.com'),
# MAGIC     ('ABC', 'ABC@dreamitcs.com'),
# MAGIC     ('CDE', 'CDE@dreamitcs.com')

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT * FROM users

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


%%sql
drop table Users

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
