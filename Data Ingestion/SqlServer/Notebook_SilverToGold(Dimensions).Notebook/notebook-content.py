# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "f8eef622-5603-4933-bcd0-2cf949b52519",
# META       "default_lakehouse_name": "Lakehouse1",
# META       "default_lakehouse_workspace_id": "3489ea4e-5cb8-402f-bc2a-ce25ce4c6e76",
# META       "known_lakehouses": [
# META         {
# META           "id": "f8eef622-5603-4933-bcd0-2cf949b52519"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Full Refresh 
address = spark.read.table("SalesLT.address")
customer = spark.read.table("SalesLT.customer")
customeraddress = spark.read.table("SalesLT.customeraddress")
product = spark.read.table("SalesLT.product")
productcategory = spark.read.table("SalesLT.productcategory")
productdescription = spark.read.table("SalesLT.productdescription")
productmodel = spark.read.table("SalesLT.productmodel")
productmodelproductdescription = spark.read.table("SalesLT.productmodelproductdescription")

address.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema","true") \
    .saveAsTable("dbo.Dim_Address")
customer.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema","true") \
    .saveAsTable("dbo.Dim_Customer")
customeraddress.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema","true") \
    .saveAsTable("dbo.Dim_Customer_Address")
product.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema","true") \
    .saveAsTable("dbo.Dim_Product")
productcategory.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema","true") \
    .saveAsTable("dbo.Dim_ProductCategory")
productdescription.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema","true") \
    .saveAsTable("dbo.Dim_ProductDescription")
productmodel.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema","true") \
    .saveAsTable("dbo.Dim_ProductModel")
productmodelproductdescription.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema","true") \
    .saveAsTable("dbo.Dim_ProductModel_ProductDescription")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# CELL ********************

#INCREMENTAL REFRESH 
# Dynamically accessing folderpath

Table_Name = ["SalesLT.address","SalesLT.customer","SalesLT.customeraddress","SalesLT.product",
"SalesLT.productcategory","SalesLT.productdescription","SalesLT.productmodel","SalesLT.productmodelproductdescription"]

for file in Table_Name:
    
    target_table = file
    if not spark.catalog.tableExists(target_table):
        print(
            f"-> Target does not exist. "
            f"Creating {target_table}"
        )
        df_clean.write \
            .format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .saveAsTable(target_table)
        print(
            f"-> Successfully created {target_table}"
        )                
    else:
        target = DeltaTable.forName(spark,target_table)
        merge_condition = " AND ".join(
        [
            f"target.{pk} = source.{pk}"
            for pk in primary_keys
        ]
        )
        # print(f"-> MERGE condition: {merge_condition}")
        (
            target.alias("target")
            .merge(
                df_clean.alias("source"),
                merge_condition
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
        print(
            f"-> Successfully MERGED {file.name} "
            f"into {target_table}"
        )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# CELL ********************

tables_df = spark.sql("SHOW TABLES")

tables_df.filter(
    tables_df.namespace == "SalesLT"
).show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SHOW TABLES in SalesLT

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC DESCRIBE HISTORY address

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }
