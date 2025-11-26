# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "e6107edf-cd6b-4426-b49b-7473f6354076",
# META       "default_lakehouse_name": "silver_layer",
# META       "default_lakehouse_workspace_id": "2fe9abb6-9a56-4088-8062-472540353376",
# META       "known_lakehouses": [
# META         {
# META           "id": "e6107edf-cd6b-4426-b49b-7473f6354076"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # **Billing _customer Transformation **

# CELL ********************

import pyspark 
import pandas as pd 
from pyspark.sql.functions import col
df=spark.read.format("delta").load("abfss://Test_ServicePrincipal@onelake.dfs.fabric.microsoft.com/bronze_layer.Lakehouse/Tables/billing_customer")
df=df.withColumn("contract_start_date",col("contract_start_date").cast("date"))
df=df.withColumn("resource_id",col("resource_id").cast("int"))
df=df.withColumn("contract_id",col("contract_id").cast("int"))
df=df.withColumn("invoice_date_paid",col("invoice_date_paid").cast("date"))
df=df.withColumn("posted_date",col("posted_date").cast("date"))
df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").option("mergeschema","true").saveAsTable("billing_customer")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # **contract_search Transformation **

# CELL ********************

import pyspark 
import pandas as pd 
from pyspark.sql.functions import col
df=spark.read.format("delta").load("abfss://Test_ServicePrincipal@onelake.dfs.fabric.microsoft.com/bronze_layer.Lakehouse/Tables/contract_search")
df=df.withColumn("start_date",col("start_date").cast("date"))
df=df.withColumn("end_date",col("end_date").cast("date"))
df=df.withColumn("estimated_revenue",col("estimated_revenue").cast("int"))
df=df.withColumn("contract_id",col("contract_id").cast("int"))
df=df.withColumn("current_balance",col("current_balance").cast("int")) 
df=df.withColumn("estimated_cost",col("estimated_cost").cast("int")) 
df=df.withColumn("tenant_id",col("tenant_id").cast("int")) 
# df=df.withColumn("posted_date",col("posted_date").cast("date"))
df.write.format("delta").mode("overwrite").option("mergeschema","true").saveAsTable("contract_search")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # **resource_list transformation**

# CELL ********************

import pyspark 
import pandas as pd 
from pyspark.sql.functions import col
df=spark.read.format("delta").load("abfss://Test_ServicePrincipal@onelake.dfs.fabric.microsoft.com/bronze_layer.Lakehouse/Tables/resource_list")
df=df.withColumn("resource_id",col("resource_id").cast("int"))
df=df.withColumn("office_phone",col("office_phone").cast("int"))
df=df.withColumn("mobile_phone",col("mobile_phone").cast("int"))
df=df.withColumn("last_activated_date",col("last_activated_date").cast("date"))
df.write.format("delta").mode("overwrite").option("mergeschema","true").saveAsTable("resource_list")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # **Digital transformation**

# CELL ********************

import pyspark 
import pandas as pd 
from pyspark.sql.functions import col
df=spark.read.format("delta").load("abfss://Test_ServicePrincipal@onelake.dfs.fabric.microsoft.com/bronze_layer.Lakehouse/Tables/digital_transformation")

df.write.format("delta").mode("overwrite").option("mergeschema","true").saveAsTable("digital_transformation")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # **ireland_holiday**

# CELL ********************

import pyspark 
import pandas as pd 
from pyspark.sql.functions import col
df=spark.read.format("delta").load("abfss://Test_ServicePrincipal@onelake.dfs.fabric.microsoft.com/bronze_layer.Lakehouse/Tables/ireland_holiday")
df=df.withColumn("date",col("date").cast("date"))
df.write.format("delta").mode("overwrite").option("mergeschema","true").saveAsTable("ireland_holiday")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
