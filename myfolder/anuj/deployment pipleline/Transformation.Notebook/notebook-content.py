# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "62532069-c1a5-4cd6-adff-47de17b3c80e",
# META       "default_lakehouse_name": "Silver_Migration",
# META       "default_lakehouse_workspace_id": "2fe9abb6-9a56-4088-8062-472540353376",
# META       "known_lakehouses": [
# META         {
# META           "id": "62532069-c1a5-4cd6-adff-47de17b3c80e"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# https://www.w3schools.com/python/python_regex.asp (regular expression)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Read a table from the lakehouse
df = spark.read.format("delta").load("Migration_Lakehouse")
df.head(2) 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if t.value == s.value and t.date == s.date:
    t.upc = s.upc
    t.date = s.date
    t.units = s.units
    t.any_other_value = s.any_other_value
elif ifnotmatched:
    t = append s

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from delta.tables import DeltaTable

table_path ="-------"

if DeltaTable.isDeltaTable(spark,table_path):
    existing_table = DeltaTable.forPath(spark, table_path)

    existing_table.alis("t").merge(
        source= df_final.alias("s"),
        condition="t.UPC" = s.UPC and t.Date = s.Date"
    ).whenMatchedUpdate(set{})

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from delta.tables import DeltaTable

# Define your Delta Lake path
table_path = "abfss://156ec263-306e-4e1d-89f8-4147afa257d7@onelake.dfs.fabric.microsoft.com/2dadaa1b-e49e-4e5b-81a6-490da423a434/Tables/Cvs"

# Check if path contains a Delta table
if DeltaTable.isDeltaTable(spark, table_path):
    existing_table = DeltaTable.forPath(spark, table_path)

    # Execute merge logic

    existing_table.alias("t").merge(
        source=df_final.alias("s"),
        condition="t.UPC = s.UPC AND t.Date = s.Date"
    ).whenMatchedUpdate(set={
        "Values": "s.Values",
        "Units": "s.Units",
        "InsertedDate":"s.InsertedDate"
    }).whenNotMatchedInsert(values={
        "UPC": "s.UPC",
        "Date": "s.Date",
        "Values": "s.Values",
        "Units": "s.Units",
        "Customer": "s.Customer",
        "InsertedDate":"s.InsertedDate"
    }).execute()

     # Show result
    existing_table.toDF().show()

else:
    print(f"The path {table_path} is not a Delta table.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# migration
#  text.write.format("delta").mode("overwrite").saveAsTable("Migration_Lakehouse")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# # text.write.mode("overwrite").format("delta").save(lakehouse_path)

# df.write.format("delta").mode("overwrite").saveAsTable("your_lakehouse_name.your_table_name")

# lakehouse_table_name = to_camel_case(table)
# lakehouse_path = f"Tables/{lakehouse_table_name}"

# print(f" Writing to Lakehouse table: {lakehouse_table_name}")
# df.write.mode("overwrite").format("delta").save(lakehouse_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# # --- Clean column names: replace space, remove Delta-invalid characters, preserve casing ---
# def clean_column_names_preserve_case(df):
#     for col in df.columns:
#         clean_col = col.replace(" ", "_")  # Replace spaces with underscores
#         clean_col = re.sub(r'[,\{\}\(\);\n\t=]', '', clean_col)  # Remove Delta-invalid characters
#         df = df.withColumnRenamed(col, clean_col)
#     return df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
