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

# # for just one table 
# df = spark.read.parquet(
#     "Files/Bronze/SalesLT.Customer/SalesLTCustomer_20260817.parquet"
# )
# # df.show()
# # df.printSchema()

# from pyspark.sql.functions import to_date

# df_clean = (
#     df
#     .filter(df.CustomerID.isNotNull())
#     .dropDuplicates()
#     .withColumn("ModifiedDate", to_date("ModifiedDate"))
# )

# # df_clean.printSchema()

# df_clean.write \
#     .format("delta") \
#     .mode("overwrite") \
#     .saveAsTable("SalesLT.customer")

# display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# CELL ********************

# #FULL REFRESH Dynamically accessing folderpath
# from pyspark.sql.functions import to_date,round,col 
# bronze_Layer = "Files/Bronze"

# bronze_folders = notebookutils.fs.ls(bronze_Layer)

# for item in bronze_folders:
#     # print(item.path)
#     bronze_folder_name = item.name.rstrip("/") 
#     print(bronze_folder_name)
#     if "." not in bronze_folder_name:
#         continue
#     schema_name = bronze_folder_name.split(".")[0]
#     table_name = bronze_folder_name.split(".")[1]
    
#     files = notebookutils.fs.ls(item.path)

#     for file in files:
#         if file.name.endswith(".parquet"):
#             print("Reading:", file.path)
#             df = spark.read.parquet(file.path)
#             df_clean = (
#                 df
#                 .dropDuplicates()
#                 .withColumn("ModifiedDate", to_date("ModifiedDate"))
#             )

#             target_table = f"{schema_name}.{table_name}"                

#             df_clean.write \
#                 .format("delta") \
#                 .mode("overwrite") \
#                 .option("overwriteSchema", "true") \
#                 .saveAsTable(target_table)

#             print(
#                 f"-> Successfully loaded {file.name} "
#                 f"into {target_table}"
#             )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# CELL ********************

table_config = {
    "Address": {
        "primary_key": ["AddressID"],
        "date_columns": ["ModifiedDate"],
        "decimal_columns": []
    },
    "CustomerAddress": {
        "primary_key": ["CustomerID", "AddressID"],
        "date_columns": ["ModifiedDate"],
        "decimal_columns": []
    },
    "SalesOrderDetail": {
        "primary_key": ["SalesOrderID", "SalesOrderDetailID"],
        "date_columns": ["ModifiedDate"],
        "decimal_columns": ["UnitPrice","UnitPriceDiscount","LineTotal"]
    },
    "SalesOrderHeader": {
        "primary_key": ["SalesOrderID"],
        "date_columns": ["OrderDate","DueDate","ShipDate","ModifiedDate"],
        "decimal_columns": ["SubTotal","TaxAmt","Freight","TotalDue"]
    },
    "Customer": {
        "primary_key": ["CustomerID"],
        "date_columns": ["ModifiedDate"],
        "decimal_columns": []
    },
    "ProductModel": {
        "primary_key": ["ProductModelID"],
        "date_columns": ["ModifiedDate"],
        "decimal_columns": []
    },
    "ProductDescription": {
        "primary_key": ["ProductDescriptionID"],
        "date_columns": [],
        "decimal_columns": []
    },
    "Product": {
        "primary_key": ["ProductID"],
        "date_columns": ["SellStartDate"],
        "decimal_columns": ["StandardCost","ListPrice","Weight"]
    },
    "ProductModelProductDescription": {
        "primary_key": ["ProductModelID","ProductDescriptionID"],
        "date_columns": ["ModifiedDate"],
        "decimal_columns": []
    },
    "ProductCategory": {
        "primary_key": ["ProductCategoryID"],
        "date_columns": ["ModifiedDate"],
        "decimal_columns": []
    }
}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#FULL REFRESH 
# Dynamically accessing folderpath
from pyspark.sql.functions import to_date,round,col 
from datetime import datetime
bronze_Layer = "Files/Bronze"
today = datetime.now().strftime("%Y%m%d")

bronze_folders = notebookutils.fs.ls(bronze_Layer)

for item in bronze_folders:
    bronze_folder_name = item.name
    print(bronze_folder_name)

    if "." not in bronze_folder_name:
        continue
    
    schema_name = bronze_folder_name.split(".")[0]
    table_name = bronze_folder_name.split(".")[1]
    
    if table_name not in table_config:
        print(f"No transformation config found for {table_name}")
        continue

    config = table_config[table_name]
    
    files = notebookutils.fs.ls(item.path)
    
    for file in files:
        
        if file.name.endswith(".parquet") and today in file.name:
            print("-> Reading today's file::", file.path)
            df = spark.read.parquet(file.path)
            df_clean=df

            # 1. Primary Key NOT NULL
            primary_keys = config["primary_key"]

            for pk in primary_keys:
                df_clean = df_clean.filter(col(pk).isNotNull())

            # 2. Remove duplicate rows
            df_clean = df_clean.dropDuplicates()

            # 3. Date transformations
            for date_column in config["date_columns"]:
                if date_column in df.columns:
                    df_clean = df_clean.withColumn(date_column,to_date(col(date_column)))

            # 4. Decimal transformations
            for decimal_column in config["decimal_columns"]:
                if decimal_column in df.columns:
                    df_clean = df_clean.withColumn(decimal_column,round(col(decimal_column), 2))

            target_table = f"{schema_name}.{table_name}"                

            df_clean.write \
                .format("delta") \
                .mode("overwrite") \
                .option("overwriteSchema", "true")\
                .saveAsTable(target_table)

            print(
                f"-> Successfully loaded {file.name} "
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

#INCREMENTAL REFRESH 
# Dynamically accessing folderpath
from pyspark.sql.functions import to_date,round,col 
from delta.tables import DeltaTable
from datetime import datetime

bronze_Layer = "Files/Bronze"
today = datetime.now().strftime("%Y%m%d")

bronze_folders = notebookutils.fs.ls(bronze_Layer)

for item in bronze_folders:
    bronze_folder_name = item.name
    print(bronze_folder_name)

    if "." not in bronze_folder_name:
        continue
    
    schema_name = bronze_folder_name.split(".")[0]
    table_name = bronze_folder_name.split(".")[1]
    
    if table_name not in table_config:
        print(f"No transformation config found for {table_name}")
        continue

    config = table_config[table_name]
    
    files = notebookutils.fs.ls(item.path)
    
    for file in files:
        
        if file.name.endswith(".parquet") and today in file.name:
            print("-> Reading today's file::", file.path)
            df = spark.read.parquet(file.path)
            df_clean=df

            # 1. Primary Key NOT NULL
            primary_keys = config["primary_key"]

            for pk in primary_keys:
                df_clean = df_clean.filter(col(pk).isNotNull())

            # 2. Remove duplicate rows
            df_clean = df_clean.dropDuplicates()

            # 3. Date transformations
            for date_column in config["date_columns"]:
                if date_column in df.columns:
                    df_clean = df_clean.withColumn(date_column,to_date(col(date_column)))

            # 4. Decimal transformations
            for decimal_column in config["decimal_columns"]:
                if decimal_column in df.columns:
                    df_clean = df_clean.withColumn(decimal_column,round(col(decimal_column), 2))

            target_table = f"{schema_name}.{table_name}"
            print(f"-> Target table: {target_table}")

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
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
