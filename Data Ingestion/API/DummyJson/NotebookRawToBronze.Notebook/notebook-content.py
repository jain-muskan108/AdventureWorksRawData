# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "e1c6bf42-b435-4b0f-9212-e993656aa3f1",
# META       "default_lakehouse_name": "API",
# META       "default_lakehouse_workspace_id": "3489ea4e-5cb8-402f-bc2a-ce25ce4c6e76",
# META       "known_lakehouses": [
# META         {
# META           "id": "e1c6bf42-b435-4b0f-9212-e993656aa3f1"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql.functions import to_date,round,col

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from datetime import datetime
from delta.tables import DeltaTable

Raw_Data = "Files/Raw"
Bronze_Data = "Files/Bronze"

today = datetime.now().strftime("%Y%m%d")

Raw_folders = notebookutils.fs.ls(Raw_Data)

for folder in Raw_folders:

    if folder.name == f"DummyJson_{today}":

        target_folder = f"{Bronze_Data}/DummyJson_{today}"

        if not notebookutils.fs.exists(target_folder):
            notebookutils.fs.mkdirs(target_folder)

        files = notebookutils.fs.ls(folder.path)

        for file in files:

            if not file.name.endswith(".parquet"):
                continue

            source_path = file.path
            table_name = file.name.replace(".parquet", "")
            target_file = f"{table_name}{today}.parquet"

            target_path = f"{target_folder}/{target_file}"

            df = spark.read.parquet(source_path)

            if not DeltaTable.isDeltaTable(spark, target_path):
                (
                    df.write
                    .format("parquet")
                    .mode("overwrite")
                    .save(target_path)
                )

            else:
                target = DeltaTable.forPath(spark,target_path)

                merge_condition = "target.id = source.id"
                (
                    target.alias("target")
                    .merge(
                        df.alias("source"),
                        merge_condition
                    )
                    .whenMatchedUpdateAll()
                    .whenNotMatchedInsertAll()
                    .execute()
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
