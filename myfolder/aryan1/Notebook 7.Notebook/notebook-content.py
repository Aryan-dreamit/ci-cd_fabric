# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "b195430e-44ac-428e-b96e-579fd42457a0",
# META       "default_lakehouse_name": "KAMBOJ",
# META       "default_lakehouse_workspace_id": "2fe9abb6-9a56-4088-8062-472540353376",
# META       "known_lakehouses": [
# META         {
# META           "id": "b195430e-44ac-428e-b96e-579fd42457a0"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Step 1: Define source and target paths

df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(source_path)


display(df)


df.write.format("delta") \
    .mode("overwrite") \
    .save(target_path)





# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE OR REPLACE TABLE SALESDELTA_TABLE
# MAGIC select * from SalesData


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.read.format("csv").option("header", "true").load("Files/SalesData.csv")
df.createOrReplaceTempView("SalesData")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

abc=spark.sql(f"""
CREATE OR REPLACE TABLE SALESDELTA_TABLE
select * from SalesData
""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
