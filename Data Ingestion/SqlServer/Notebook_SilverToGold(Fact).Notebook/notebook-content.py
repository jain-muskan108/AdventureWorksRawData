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

from pyspark.sql.functions import (
    col,
    sum,
    countDistinct,
    avg,
    round,
    when,
    datediff,
    max,
    min ,
    count
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

sales_order_header = spark.read.table("SalesLT.SalesOrderHeader")
sales_order_detail = spark.read.table("SalesLT.SalesOrderDetail")

fact_sales = (
    sales_order_detail.alias("detail")
    .join(
        sales_order_header.alias("header"),
        col("detail.SalesOrderID") == col("header.SalesOrderID"),
        "left"
    )
)

fact_sales = fact_sales.select(
    col("detail.SalesOrderID").alias("SalesOrderID"),
    col("detail.SalesOrderDetailID").alias("SalesOrderDetailID"),
    col("detail.ProductID").alias("ProductID"),
    col("detail.OrderQty").alias("OrderQty"),
    col("detail.UnitPrice").alias("UnitPrice"),
    col("detail.UnitPriceDiscount").alias("UnitPriceDiscount"),
    col("detail.LineTotal").alias("LineTotal"),
    col("detail.SalesOrderDetailID").alias("SalesOrderDetailID"),
    

    col("header.CustomerID").alias("CustomerID"),
    col("header.OrderDate").alias("OrderDate"),
    col("header.DueDate").alias("DueDate"),
    col("header.ShipDate").alias("ShipDate"),
    col("header.SubTotal").alias("SubTotal"),
    col("header.TaxAmt").alias("TaxAmt"),
    col("header.Freight").alias("Freight"),
    col("header.TotalDue").alias("TotalDue"),
    col("header.OnlineOrderFlag").alias("OnlineOrderFlag"),
    col("header.ShipMethod").alias("ShipMethod"),
    col("header.PurchaseOrderNumber").alias("PONumber"),
    col("header.AccountNumber").alias("AccountNumber"),
    col("header.ShipToAddressID").alias("ShipToAddressID"),
    col("header.BillToAddressID").alias("BillToAddressID")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

fact_sales = (
    fact_sales
        .withColumn("GrossSales",
            round(
                col("UnitPrice") * col("OrderQty"),
                2
            )
        )
        .withColumn("DiscountAmount",
            round(
                col("GrossSales")* col("UnitPriceDiscount"),
                2
            )
        )
        .withColumn("NetSales",
            round(
                col("GrossSales") - col("DiscountAmount"),
                2
            )
        )
        .withColumn("ShippingDays",
            datediff(
                col("ShipDate"),col("OrderDate")
            )
        )
        .withColumn("ShipmentStatus",
            when(
                col("ShipDate").isNull(),"Not Shipped"
            )
            .when(
                col("ShipDate") <= col("DueDate"),"On Time"
            )
            .otherwise("Late")
        )
        .withColumn("SalesChannel",
            when(
                col("OnlineOrderFlag") == True,
                "Online"
            )
            .otherwise("Offline")
        )
        .withColumn("DaysLate",
            when(
                col("ShipDate") > col("DueDate"),
                datediff(
                    col("ShipDate"),
                    col("DueDate")
                )
            ).otherwise(0)
        )

    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

fact_sales = fact_sales.repartition(50)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# CELL ********************

display(fact_sales.rdd.getNumPartitions())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# CELL ********************

display(fact_sales)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# CELL ********************

sales_kpis = fact_sales.agg(
    # Sales
    round(sum("LineTotal"), 2).alias("TotalSales"),
    round(sum("GrossSales"), 2).alias("GrossSales"),
    round(sum("DiscountAmount"), 2).alias("TotalDiscount"),
    round(sum("NetSales"), 2).alias("NetSales"),
    # Orders
    countDistinct("SalesOrderID").alias("TotalOrders"),
    # Units
    sum("OrderQty").alias("TotalUnitsSold"),
    # Averages
    round(
        sum("LineTotal") /
        countDistinct("SalesOrderID"),
        2
    ).alias("AverageOrderValue"),

    round(
        sum("LineTotal") /
        sum("OrderQty"),
        2
    ).alias("AverageSellingPrice")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

discount_kpi = fact_sales.agg(
    round(
        sum("DiscountAmount"),
        2
    ).alias("TotalDiscount"),
    round(
        (
            sum("DiscountAmount") /
            sum("GrossSales")
        ) * 100,
        2
    ).alias("DiscountPercentage") ,
    sum(
        when(col("UnitPriceDiscount") > 0, 1).otherwise(0)
    ).alias("DiscountedLines")

)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Repeat Customers
customer_orders = (
    fact_sales
    .groupBy("CustomerID")
    .agg(
        countDistinct("SalesOrderID").alias("OrderCount")
    )
)

repeat_customers = customer_orders.filter(
    col("OrderCount") > 1
)

repeat_customer_kpi = customer_orders.agg(
    count("*").alias("TotalCustomers"),
    sum(
        when(col("OrderCount") > 1, 1)
        .otherwise(0)
    ).alias("RepeatCustomers")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(fact_sales.rdd.getNumPartitions())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# CELL ********************

display(fact_sales.count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# CELL ********************

product_kpi = (
    fact_sales
    .groupBy("ProductID")
    .agg(
        sum("OrderQty").alias("UnitsSold"),
        round(sum("LineTotal"), 2).alias("Sales"),
        round(sum("GrossSales"), 2).alias("GrossSales"),
        round(sum("DiscountAmount"), 2).alias("Discount"),
        round(sum("NetSales"), 2).alias("NetSales"),
        countDistinct("SalesOrderID").alias("Orders"),
        round(
            sum("NetSales") /
            countDistinct("SalesOrderID"),
            2
        ).alias("AverageOrderValue")
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

order_shipping = (
    fact_sales
    .select(
        "SalesOrderID",
        "ShipmentStatus",
        "DaysLate",
        "ShippingDays",
        "ShipMethod",
        "Freight"
    )
    .dropDuplicates(["SalesOrderID"])
)

shipping_kpi = order_shipping.agg(
    # TotalShippedOrders
    countDistinct("SalesOrderID").alias("TotalShippedOrders"),
    # OnTimeOrders
    countDistinct(
        when(
            col("ShipmentStatus") == "On Time",
            col("SalesOrderID")
        )
    ).alias("OnTimeOrders"),
    # LateOrders
    countDistinct(
        when(
            col("ShipmentStatus") == "Late",
            col("SalesOrderID")
        )
    ).alias("LateOrders"),
    # AverageShippingDays
    round(
        avg("ShippingDays"),
        2
    ).alias("AverageShippingDays"),
    # AverageDaysLate
    round(
        avg(
            when(
                col("DaysLate") > 0,
                col("DaysLate")
            )
        ),
        2
    ).alias("AverageDaysLate"),
    # AverageFreight
    round(
        avg("Freight"),
        2
    ).alias("AverageFreight")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

fact_sales.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("dbo.fact_sales")
    
sales_kpis.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("dbo.sales_kpi")

discount_kpi.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema","true") \
    .saveAsTable("dbo.discount_kpi")

product_kpi.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema","true") \
    .saveAsTable("dbo.product_kpi")

shipping_kpi.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema","true") \
    .saveAsTable("dbo.shipping_kpi")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run Notebook_SilverToGold(Dimensions)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
