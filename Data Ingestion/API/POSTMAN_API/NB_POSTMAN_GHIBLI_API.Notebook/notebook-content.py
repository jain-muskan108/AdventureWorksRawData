# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "446036ab-fe28-4baf-8ca8-f06e57bfba2d",
# META       "default_lakehouse_name": "POSTMAN_GHIBLI_API",
# META       "default_lakehouse_workspace_id": "3489ea4e-5cb8-402f-bc2a-ce25ce4c6e76",
# META       "known_lakehouses": [
# META         {
# META           "id": "446036ab-fe28-4baf-8ca8-f06e57bfba2d"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql.functions import *
from datetime import datetime

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

table_config ={
    "films" :
    {
        "columns_to_drop": ["people","species","locations","vehicles","url"],
        "str_columns": ["id","title","original_title","original_title_romanised","image","movie_banner","description","director","producer"],
        "int_columns": ["release_date","running_time","rt_score"]
    },
    "locations":
    {
        "columns_to_drop": ["films","residents","url"],
        "str_columns": ["id","name","climate","terrain"],
        "int_columns": ["surface_water"]

    },
    "people":
    {
        "columns_to_drop": ["films","species","url"],
        "str_columns": ["id","name","gender","eye_color","hair_color"],
        "int_columns": ["age"]
    },
    "species":
    {
        "columns_to_drop": ["people","films","url"],
        "str_columns": ["id","name","classification","eye_colors","hair_colors"],
        "int_columns": []
    },
    "vehicles":
    {
        "columns_to_drop": ["pilot","films","url"],
        "str_columns": ["id","name","description","vehicle_class"],
        "int_columns": ["length"]
    }
}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

bronze_Layer = "Files/Bronze"
today = datetime.now().strftime("%Y%m%d")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

folders = notebookutils.fs.ls(bronze_Layer)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

for item in folders:
    folder = item.name
    
    files = notebookutils.fs.ls(item.path)    
    
    for file in files:
        if not file.name.endswith(".parquet") and today not in file.name :
            continue

        table_name = file.name.split("_")[0]
        # print(table_name)

        if table_name not in table_config:
            continue

        df = spark.read.parquet(file.path)
        df_clean = df 

        for column in config["columns_to_drop"]:
            if column in df_clean.columns:
                df_clean = df_clean.drop(column)
        
        for column in config["str_columns"]:
                df_clean = df_clean.withColumn(column, col(column).cast("string"))

        for column in config["int_columns"]:
                df_clean = df_clean.withColumn(column, col(column).cast("int"))

        df_clean = df_clean.withColumn("HashKey", concat(*[col(c) for c in df_clean.columns]))

        df_clean = df_clean.withColumn("ingestion_date",current_timestamp())

        target_table = f"Silver.{table_name}"

        # df_clean.write \
        #     .format("delta") \
        #     .mode("overwrite") \
        #     .saveAsTable(target_table)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

source_path = "abfss://Workspace1@onelake.dfs.fabric.microsoft.com/POSTMAN_GHIBLI_API.Lakehouse/Files/Bronze/"

folder_name  = ["films","locations","people","species","vehicles"]

df_lt = {}

for folder in folder_name:

    path = source_path+folder
    
    files = notebookutils.fs.ls(path)

    for file in files:

        if not file.name.endswith(".parquet") or today not in file.name:
            continue

        df_lt[folder] = spark.read.format("parquet").load(file.path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# print(df_lt)
for key, value in df_lt.items():

        table_name = key

        if table_name not in table_config:
            continue

        for column in config["columns_to_drop"]:
            if column in df[value].columns:
                df_lt[value] = df_lt[value].drop(column)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

for folder in folder_name:
    files = notebookutils.fs.ls(folder.path)

    for file in files:

        table_name = file.name.split("_")[0]

        if table_name not in table_config:
            continue
        
        for column in config["str_columns"]:
                df_clean = df_clean.withColumn(column, col(column).cast("string"))

        for column in config["int_columns"]:
                df_clean = df_clean.withColumn(column, col(column).cast("int"))
        

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

for folder in folder_name:
    files = notebookutils.fs.ls(folder.path)

    for file in files:

        table_name = file.name.split("_")[0]

        if table_name not in table_config:
            continue

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
