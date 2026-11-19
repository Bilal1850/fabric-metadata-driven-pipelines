# Reading and writing Fabric Warehouse tables from a Fabric Spark notebook
# using the Spark connector for Fabric Data Warehouse (synapsesql).
# Docs: https://learn.microsoft.com/fabric/data-engineering/spark-data-warehouse-connector
# Table names below are generic placeholders.

import com.microsoft.spark.fabric
from com.microsoft.spark.fabric.Constants import Constants

# Read from a Lakehouse Delta table
df = spark.read.format("delta").load("Tables/cl_clearing_table")

# Read from a Fabric Warehouse table
df_read = spark.read.synapsesql("DataOpsWarehouse.dbo.cl_clearing_table")
df_read.show()

# Write to a Fabric Warehouse table (modes: overwrite, append, ignore)
df.write.mode("overwrite").synapsesql("DataOpsWarehouse.dbo.cl_clearing_table")
