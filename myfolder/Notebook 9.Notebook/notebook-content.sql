-- Fabric notebook source

-- METADATA ********************

-- META {
-- META   "kernel_info": {
-- META     "name": "synapse_pyspark"
-- META   },
-- META   "dependencies": {
-- META     "lakehouse": {
-- META       "default_lakehouse": "e9873df5-dab8-43ac-bb56-dfb4845f5add",
-- META       "default_lakehouse_name": "aryan11",
-- META       "default_lakehouse_workspace_id": "2fe9abb6-9a56-4088-8062-472540353376",
-- META       "known_lakehouses": [
-- META         {
-- META           "id": "e9873df5-dab8-43ac-bb56-dfb4845f5add"
-- META         }
-- META       ]
-- META     }
-- META   }
-- META }

-- CELL ********************

-- Welcome to your new notebook
-- Type here in the cell editor to add code!
select * from dim_salesdelta_table

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- MAGIC %%pyspark
-- MAGIC from pyspark.sql.functions import col , cast
-- MAGIC df=spark.read.format('delta').load("abfss://Test_ServicePrincipal@onelake.dfs.fabric.microsoft.com/aryan11.Lakehouse/Tables/dbo/salesdelta_table")
-- MAGIC df=df.withColumn("OrderDate",col("OrderDate").cast("Date"))
-- MAGIC df=df.withColumn("Quantity",col("Quantity").cast("int"))
-- MAGIC df=df.withColumn("Price",col("Price").cast("int"))
-- MAGIC df.write.format("delta") .mode("overwrite") .option("overwriteSchema", "true") .saveAsTable("salesdelta_table")
-- MAGIC 


-- METADATA ********************

-- META {
-- META   "language": "python",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- MAGIC %%pyspark
-- MAGIC from pyspark.sql.functions import col , cast
-- MAGIC df=spark.read.format('delta').load("abfss://Test_ServicePrincipal@onelake.dfs.fabric.microsoft.com/aryan11.Lakehouse/Tables/dbo/salesdelta_table")
-- MAGIC 
-- MAGIC df.write.format("delta") .mode("overwrite") .option("overwriteSchema", "true") .saveAsTable("dim_salesdelta_table")

-- METADATA ********************

-- META {
-- META   "language": "python",
-- META   "language_group": "synapse_pyspark"
-- META }

-- MARKDOWN ********************

-- # **scd type1**

-- CELL ********************

update  dim_salesdelta_table 
set CustomerName ="aryan"
where OrderID=1001;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- MARKDOWN ********************

-- # **scd type 2**

-- CELL ********************

update  dim_salesdelta_table 
set OrderDate = CURRENT_DATE
where OrderID=1001;
insert into dim_salesdelta_table values (1006,"kamboj","mobile",4,1000,CURRENT_DATE,"india")

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- MARKDOWN ********************

-- # **scd type3**

-- CELL ********************

alter dim_sales_table 
ADD COLUMN CurrentDate datetime;


-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************


-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }
