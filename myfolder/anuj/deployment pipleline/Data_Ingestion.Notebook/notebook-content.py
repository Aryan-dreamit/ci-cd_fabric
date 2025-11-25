# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "45cf811a-6020-4c2c-8dab-eb53a8aa7511",
# META       "default_lakehouse_name": "Migration_Lakehouse",
# META       "default_lakehouse_workspace_id": "2fe9abb6-9a56-4088-8062-472540353376",
# META       "known_lakehouses": [
# META         {
# META           "id": "45cf811a-6020-4c2c-8dab-eb53a8aa7511"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

import re

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# # importing data from the csv file
 
# import pandas as pd 

# data  = pd.read_csv("/lakehouse/default/Files/raw data/unstructured_data_unclean.csv")
# data.head(5)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

text = pd.DataFrame("unstructured_data")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#  making a cpoy of the data
text = data

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# to display names of the columns from raw data
for i in text.columns:
    print(i)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

text.head(2)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": false,
# META   "editable": true
# META }

# CELL ********************

text.dtypes

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# cap the names of the columns 

# for i in text.columns:
#     a = i.title().strip().replace(" ","_")
#     print(a)
# cap can be changed bases on the bases of demand of the customer  
text_list = []
for j in text.columns:
    i = re.sub(r'[^a-zA-Z0-9_\s]', '',j)
    a = i.title().strip()
    b = a.replace(" ","_")
    print(b)
    text_list += [b]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

text_list

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

for i in text_list:
    print(type(i))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# z = pd.DataFrame(text_list)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

text.columns

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# def cleaning(m):
for t in text.columns:
    print(t)
    i = re.sub(r'[^a-zA-Z0-9_\s]', '',t)
    a = i.title().strip()
    b = a.replace(" ","_")
    print(b)
    # text_list += [b]
    df = t.replace(t,b)
    print(df)
    # return df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# for j in text.columns:
#     i = re.sub(r'[^a-zA-Z0-9_\s]', '',j)
#     a = i.title().strip()
#     b = a.replace(" ","_")
#     print(b)
#     text_list += [b]

def cleaning(m):
    for t in text.columns:
        i = re.sub(r'[^a-zA-Z0-9_\s]', '',t)
        a = i.title().strip()
        b = a.replace(" ","_")
        print(b)
        # text_list += [b]
        df = t.withColumnRenamed(t,b)
    return df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import SparkSession

# Initialize SparkSession
spark = SparkSession.builder.appName("CreateDataFrame").getOrCreate()

clean = spark.createDataFrame(text)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

for i in clean:
    print(i)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

n = cleaning(clean)
for i in n:
    print(i)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

for i in n:
    print(n)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# clean.head(2)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# clean.write.format("delta").mode("overwrite").saveAsTable("Migration_Lakehouse")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# for table in table_list:
#     print(f"Reading table: {table}")

#     # Read from Azure SQL
#     df = spark.read.jdbc(
#         url=jdbcUrl,
#         table=table,
#         properties=connectionProperties
#     )

    # Clean column names
# df = clean_column_names_preserve_case(df)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
