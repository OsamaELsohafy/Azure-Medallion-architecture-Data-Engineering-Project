# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC # SLIVER LAYER SCRIPT

# COMMAND ----------

# MAGIC %md
# MAGIC ### ACCESS DATA USING APP

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.awstorgedatalacke.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.awstorgedatalacke.dfs.core.windows.net", "")
spark.conf.set("fs.azure.account.oauth2.client.id.awstorgedatalacke.dfs.core.windows.net", "")
spark.conf.set("fs.azure.account.oauth2.client.secret.awstorgedatalacke.dfs.core.windows.net","")
spark.conf.set("fs.azure.account.oauth2.client.endpoint.awstorgedatalacke.dfs.core.windows.net", "")

# COMMAND ----------

# MAGIC %md
# MAGIC ### DATA LOADING

# COMMAND ----------

# MAGIC %md
# MAGIC #### Display ALL Data

# COMMAND ----------

df_all = dbutils.fs.ls("abfss://bronze@awstorgedatalacke.dfs.core.windows.net/")

display(df_all)

# COMMAND ----------

df_customers = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("abfss://bronze@awstorgedatalacke.dfs.core.windows.net/AdventureWorks_Customers.csv")

# COMMAND ----------

df_customers.display()

# COMMAND ----------

df_calender  = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("abfss://bronze@awstorgedatalacke.dfs.core.windows.net/AdventureWorks_Calendar.csv")

# COMMAND ----------

df_pro = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("abfss://bronze@awstorgedatalacke.dfs.core.windows.net/AdventureWorks_Products.csv")

# COMMAND ----------

df_pro_category  = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("abfss://bronze@awstorgedatalacke.dfs.core.windows.net/AdventureWorks_Product_Categories.csv")

# COMMAND ----------

df_pro_sub_category  = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("abfss://bronze@awstorgedatalacke.dfs.core.windows.net/AdventureWorks_Product_Subcategories.csv")

# COMMAND ----------

df_returns  = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("abfss://bronze@awstorgedatalacke.dfs.core.windows.net/AdventureWorks_Returns.csv")

# COMMAND ----------

df_Sales  = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("abfss://bronze@awstorgedatalacke.dfs.core.windows.net/AdventureWorks_Sales*")

# COMMAND ----------

df_Territories = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("abfss://bronze@awstorgedatalacke.dfs.core.windows.net/AdventureWorks_Territories.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC ## TRANSFORMATIONS

# COMMAND ----------

# MAGIC %md
# MAGIC ### Calendar

# COMMAND ----------

df_calender.display()

# COMMAND ----------

df_cal = df_calender.withColumn("Month" ,month(col("Date")))\
    .withColumn("Year"  ,year(col("Date")))

df_cal.display()    

# COMMAND ----------

# MAGIC %md
# MAGIC ### Loading data into sliver container

# COMMAND ----------

df_cal.write.format("parquet").mode("append")\
    .option("path", "abfss://silver@awstorgedatalacke.dfs.core.windows.net/AdventureWorks_Calendar")\
    .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Customer

# COMMAND ----------

df_customers.display()

# COMMAND ----------

from pyspark.sql.functions import concat_ws, col

df_cus = df_customers.withColumn(
    "FullName", 
    concat_ws(' ', col("Prefix"), col('FirstName'), col('LastName'))
)
df_cus.display()

# COMMAND ----------

df_cus.write.format("parquet").mode("append")\
    .option("path", "abfss://silver@awstorgedatalacke.dfs.core.windows.net/AdventureWorks_Customers")\
    .save()

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Production

# COMMAND ----------

df_pro.display()

# COMMAND ----------

from pyspark.sql.functions import split, col

df_product = df_pro.withColumn('ProductSKU', split(col("ProductSKU"), '-')[0])\
               .withColumn('ProductName', split(col("ProductName"), ' ')[0])

display(df_product)

# COMMAND ----------

df_product.write.format("parquet").mode("append")\
    .option("path", "abfss://silver@awstorgedatalacke.dfs.core.windows.net/AdventureWorks_Products")\
    .save()

# COMMAND ----------

df_pro_sub_category.write.format("parquet").mode("append")\
    .option("path", "abfss://silver@awstorgedatalacke.dfs.core.windows.net/AdventureWorks_SubCategoryies")\
    .save()

# COMMAND ----------

df_pro_category.display()

# COMMAND ----------

df_pro_category.write.format("parquet").mode("append")\
    .option("path", "abfss://silver@awstorgedatalacke.dfs.core.windows.net/AdventureWorks_Product_Categories")\
    .save()

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Returns

# COMMAND ----------

df_returns.display()

# COMMAND ----------

df_returns.write.format("parquet").mode("append")\
    .option("path", "abfss://silver@awstorgedatalacke.dfs.core.windows.net/AdventureWorks_Returns")\
    .save()

# COMMAND ----------

df_Territories.write.format("parquet").mode("append")\
    .option("path", "abfss://silver@awstorgedatalacke.dfs.core.windows.net/AdventureWorks_Territories")\
    .save()

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Sales Data 

# COMMAND ----------

df_Sales.display()

# COMMAND ----------

df_sal = df_Sales.withColumn("StockDate", to_timestamp("StockDate")) \
    .withColumn("OrderNumber", regexp_replace(col("OrderNumber"),"S","T")) \
    .withColumn("multiply", col("OrderLineItem") * col("OrderQuantity"))

display(df_sal)

# COMMAND ----------

df_sal.write.format("parquet").mode("append")\
    .option("path", "abfss://silver@awstorgedatalacke.dfs.core.windows.net/AdventureWorks_Sales")\
    .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sales Analysis

# COMMAND ----------

## total Sales for each day 
df_sal.groupby("OrderDate").agg(count("OrderNumber")).alias("Total Sales").display()

# COMMAND ----------

df_pro_category.display()

# COMMAND ----------

# Assuming df_Sales and df_products are Spark DataFrames
df_sal.createOrReplaceTempView("df_sal")
df_product.createOrReplaceTempView("df_product")

#SQL Query
query = """
SELECT * 
FROM df_sal s 
full JOIN df_product p 
ON s.ProductKey = p.productKey
"""
result = spark.sql(query)
display(result)

# COMMAND ----------

