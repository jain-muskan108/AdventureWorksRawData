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

bronze_folders = notebookutils.fs.ls("Files/Bronze")

for item in bronze_folders:
    bronze_folder_name = item.name
    print(bronze_folder_name)

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
