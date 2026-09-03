# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

from pyspark.sql.functions import *

sales_data = [
    (1001, "C001", "P001", 2, 500,  "2026-01-05", "Delhi",     "Online",  "Completed"),
    (1002, "C002", "P002", 5, 200,  "2026-01-07", "Mumbai",    "Store",   "Completed"),
    (1003, "C001", "P003", 1, 1000, "2026-01-10", "Delhi",     "Online",  "Completed"),
    (1004, "C003", "P001", 3, 500,  "2026-01-12", "Bangalore", "Store",   "Completed"),
    (1005, "C002", "P002", 5, 200,  "2026-01-15", "Mumbai",    "Online",  "Completed"),
    (1006, "C004", "P004", 2, 750,  "2026-01-18", "Chennai",   "Online",  "Pending"),
    (1007, "C005", "P005", 4, 300,  "2026-01-20", "Delhi",     "Store",   "Completed"),
    (1008, "C003", "P003", 2, 1000, "2026-01-22", "Bangalore", "Online",  "Cancelled"),
    (1009, "C001", "P002", 10, 200, "2026-01-25", "Delhi",     "Online",  "Completed"),
    (1010, "C006", "P001", 1, 500,  "2026-01-28", "Pune",      "Store",   "Completed"),
    (1011, "C007", "P006", 3, 450,  "2026-02-02", "Mumbai",    "Online",  "Completed"),
    (1012, "C002", "P004", 2, 750,  "2026-02-05", "Mumbai",    "Store",   "Completed"),
    (1013, "C004", "P005", 6, 300,  "2026-02-08", "Chennai",   "Online",  "Pending"),
    (1014, "C005", "P003", 1, 1000, "2026-02-10", "Delhi",     "Store",   "Completed"),
    (1015, "C003", "P002", 8, 200,  "2026-02-12", "Bangalore", "Online",  "Completed"),
    (1016, "C001", "P006", 2, 450,  "2026-02-15", "Delhi",     "Store",   "Completed"),
    (1017, "C006", "P004", 3, 750,  "2026-02-18", "Pune",      "Online",  "Cancelled"),
    (1018, "C007", "P005", 5, 300,  "2026-02-20", "Mumbai",    "Store",   "Completed"),
    (1019, "C002", "P001", 4, 500,  "2026-02-22", "Mumbai",    "Online",  "Completed"),
    (1020, "C004", "P006", 7, 450,  "2026-02-25", "Chennai",   "Online",  "Completed"),

    # Duplicate order for practicing dropDuplicates
    (1020, "C004", "P006", 7, 450,  "2026-02-25", "Chennai",   "Online",  "Completed"),

    # More data
    (1021, "C008", "P007", 2, 1200, "2026-03-01", "Delhi",     "Online",  "Completed"),
    (1022, "C001", "P005", 3, 300,  "2026-03-03", "Delhi",     "Store",   "Completed"),
    (1023, "C009", "P002", 6, 200,  "2026-03-05", "Pune",      "Online",  "Pending"),
    (1024, "C005", "P007", 1, 1200, "2026-03-08", "Delhi",     "Store",   "Completed"),
    (1025, "C010", "P004", 4, 750,  "2026-03-10", "Mumbai",    "Online",  "Completed"),
]

sales_columns = ["SalesOrderID","CustomerID","ProductID","Quantity","UnitPrice","OrderDate",
"City","SalesChannel","OrderStatus"]

sales_df = spark.createDataFrame(sales_data, sales_columns)

sales_df = sales_df.withColumn(
    "OrderDate",
    to_date("OrderDate")
)

customer_data = [
    ("C001", "Rahul",  "Sharma", "Delhi",     "Gold"),
    ("C002", "Amit",   "Verma",  "Mumbai",    "Silver"),
    ("C003", "Priya",  "Singh",  "Bangalore", "Gold"),
    ("C004", "Neha",   "Gupta",  "Chennai",   "Silver"),
    ("C005", "Rohit",  "Kumar",  "Delhi",     "Bronze"),
    ("C006", "Ankit",  "Mehta",  "Pune",      "Gold"),
    ("C007", "Sneha",  "Jain",   "Mumbai",    "Silver"),
    ("C008", "Vikas",  "Rao",    "Delhi",     "Gold"),
    ("C009", "Pooja",  "Nair",   "Pune",      "Bronze"),
    ("C010", "Karan",  "Malhotra","Mumbai",   "Gold")
]

customer_columns = ["CustomerID","FirstName","LastName","CustomerCity","CustomerSegment"]

customer_df = spark.createDataFrame(
    customer_data,
    customer_columns
)

product_data = [
    ("P001", "Laptop",       "Electronics", 500),
    ("P002", "Keyboard",     "Electronics", 200),
    ("P003", "Monitor",      "Electronics", 1000),
    ("P004", "Headphones",   "Accessories", 750),
    ("P005", "Mouse",        "Accessories", 300),
    ("P006", "Webcam",       "Accessories", 450),
    ("P007", "Tablet",       "Electronics", 1200)
]

product_columns = [ "ProductID","ProductName","Category","StandardPrice"]

product_df = spark.createDataFrame(
    product_data,
    product_columns
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ----------------------------
# 
# ### SELECT
# Choose specific columns from a dataframe 
# 
# Use select() when you want to:
# - choose columns
# - create calculated columns temporarily
# - rename columns in the output
# - apply expressions
# 
# 
# SYNTAX : **_df.select("column1",column2")_**
# 
# ----------------------------

# CELL ********************

sales_df.select(
    "SalesOrderID",
    "CustomerID",
    "Quantity"
).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ----------------------------
# **_from spark.sql.functions import col_**
# 
# **_df.select(col(column1),col(column2))_**
# 
# ----------------------------

# CELL ********************

from pyspark.sql.functions import col

sales_df.select(
    "SalesOrderID",
    (col("Quantity") * col("UnitPrice")).alias("SalesAmount")
).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ----------------------------
# 
# SELECT ALL
# 
# ----------------------------

# CELL ********************

sales_df.select("*").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ----------------------------
# 
# ### FILTER
# 
# filter() = WHERE in SQL
# 
# SYNTAX : _**df.filter(condition)**_
# 
# ----------------------------

# CELL ********************

sales_df.filter(
    col("Quantity") > 5
).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# --------------------------
# 
# USE :
# 
# **& | ~**
# 
# Instead of :
# 
# **and or not**
# 
# --------------------------


# CELL ********************

sales_df.filter(
    (col("Quantity") > 5) &
    (col("OrderStatus") == "Completed")
).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

sales_df.filter(
    (col("City") == "Delhi") |
    (col("City") == "Mumbai")
).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# --------------------------
# 
# isin()
# 
# --------------------------


# CELL ********************

sales_df.filter(
    ~col("OrderStatus").isin("Cancelled")
).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# --------------------------
# NULL
# 
# --------------------------


# CELL ********************

sales_df.filter(
    col("City").isNull()
).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

sales_df.filter(
    col("City").isNotNull()
).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# --------------------------
# ### where
# 
# where() and filter() are essentially the same operation.
# 
# When to use which?
# 
# **No meaningful performance difference.**

# CELL ********************

sales_df.where(
    col("Quantity") > 5
).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# -------------------------
# 
# ### withColumn
# 
# Used to:
# 
# - create a new column
# - modify an existing column
# 
# SYNTAX: _**df.withColumn("new_column", expression)**_

# CELL ********************

sales_df = sales_df.withColumn(
    "SalesAmount",
    col("Quantity") * col("UnitPrice")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# --------------------------
# ### CONDITIONAL STATEMENT 
# 
# .WHEN 
# 
# .otherwise

# CELL ********************

sales_df = sales_df.withColumn(
    "OrderPriority",
    when (col("Quantity") >=7,"High")
    .when(col("Quantity") >=5,"Medium")
    .otherwise ("Low")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ------------------------
# ##### MODIFY

# CELL ********************

sales_df = sales_df.withColumn(
    "Quantity",
    col("Quantity") * 2
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ---------------------
# ##### Cast Datatype

# CELL ********************

sales_df = sales_df.withColumn(
    "Quantity",
    col("Quantity").cast("integer")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# --------------------
# ##### Renaming Column 
# 
# df.withColumnRenamed("old_name", "new_name")
# 
# If the column doesn't exist, Spark generally doesn't throw an error; the DataFrame remains unchanged.

# CELL ********************

sales_df = sales_df.withColumnRenamed(
    "UnitPrice",
    "SellingPrice"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ------
# Multiple renames 

# CELL ********************

sales_df = (
    sales_df.withColumnRenamed("City","CustomerCity")
    .withColumnRenamed("Quantity","OrderQuantity")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# -----
# 
# ### DROP
# df.drop("column")

# CELL ********************

sales_df.drop("salesChannel")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ----
# _**sales_df.drop("City")**_ does not modify sales_df.
# 
# You need:
# 
# _**sales_df = sales_df.drop("City")**_ to modify sales_df
# 
# unless you're simply displaying the result.

# MARKDOWN ********************

# --------
# 
# ### distinct
# 
# Removes duplicate entire rows.
# If two rows are completely identical, one will be removed.
# 
# SYNTAX : _**df.distinct()**_

# CELL ********************

sales_df.distinct().show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ----------
# ### dropDuplicates
# 
# more flexible than distinct 
# 
# SYNTAX : _**df.dropDuplicates(["Column"] ).show()**_

# CELL ********************

sales_df.dropDuplicates(
    ["SalesOrderID"]
).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ------
# ### groupBy
# 
# used for aggregation 
# 
# SYNTAX : _**df.groupBy("column")**_

# CELL ********************

sales_df.groupBy(
    "CustomerCity"
).count().show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

sales_df.groupBy(
    "CustomerCity",
    "ProductID"
).count().show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

sales_df.groupBy(
    "CustomerID"
).sum("SalesAmount").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# -----------
# 
# ### AGGREGATIONS
# 
# SYNTAX : 
# from pyspark.sql.functions import *
# 
# df.groupBy("Column").agg(
#     sum("Column 1").alias("New_Name")
# ).show()

# CELL ********************

sales_df.agg(
    sum("SalesAmount").alias("TotalSales"),
    avg("SalesAmount").alias("AverageSales"),
    max("SalesAmount").alias("MaximumSale")
).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import *
sales_df.groupBy("CustomerCity").agg(
    sum("SalesAmount").alias("TotalSales")
).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# -------
# ### JOIN
# 
#     df1.join(
# 
#         df2,
# 
#         condition,
# 
#         join_type
# 
#     )
# 
# Pyspark supports joins - 
# inner , left , right , outer , full , left_semi , left_anti , cross
# 
# Left anti - Find sales records that don't have a matching customer.
# 
# A-B
# 
# Left semi - Find sales records where a customer exists.
# 


# CELL ********************

sales_customer_df = sales_df.join(
    customer_df ,
    sales_df.CustomerID == customer_df.CustomerID ,
    "left"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# -----
# Better syntax when column name is identical

# CELL ********************

sales_customer_df = sales_df.join(
    customer_df,
    on="CustomerID",
    how="inner"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ---------
# 
# ### orderBy
# 
# Sorts Dataframe 
# 
# By default - Ascending 

# CELL ********************

sales_df.orderBy(
    "SalesAmount"
).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

sales_df.orderBy(
    col("SalesAmount").desc()
).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ------
# ###### Sort() and orderBy() are essentially equivalent.

# CELL ********************

sales_df.sort(
    col("SalesAmount").desc()
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# -------
# ###### Show columns without truncating strings 

# CELL ********************

sales_df.show(
    5,
    truncate=False
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ------------
# **VERTICAL DISPLAY** 

# CELL ********************

sales_df.show(
    5,
    vertical=True
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# -----------------
# #### PRACTISE
# 
# ###### L1 Basics

# CELL ********************

# Display only SalesOrderID, CustomerID and ProductID.

sales_df.select("SalesOrderID","CustomerID","ProductID").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Find orders where Quantity > 5
sales_df.filter(
    col("Quantity")>5
).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Find orders from Delhi
sales_df.filter(
    col("City").isin("Delhi")
).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Find completed orders
sales_df.filter(
    col("orderStatus") == "Completed"
).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Find orders where Quantity > 5 AND OrderStatus = Completed
sales_df.filter(
    (col("Quantity")>5) &
    (col("orderStatus") == "Completed")
).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Rename UnitPrice → SellingPrice
sales_df.withColumnRenamed(
    "UnitPrice" ,"SellingPrice"
).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Remove SalesChannel
sales_df.drop("SalesChannel").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Find total number of orders
sales_df.select(
    "SalesOrderID"
).distinct().count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# First 10 rows 
sales_df.show(10)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Print Schema 
sales_df.printSchema()

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
