# Databricks notebook source
spark.conf.set('spark.sql.ansi.enabled', 'false')

# COMMAND ----------

# handle bp records
from pyspark.sql import SparkSession
from pyspark.sql.functions import col,month,substring


spark = SparkSession.builder.appName("patbprecs").getOrCreate()
pat_bp_df=spark.read.table("workspace.default.bp_records_bp_records")
pat_bp_df.distinct().show()
pat_bp_df.groupBy("Physician Name").count().show
#pat_bp_df.groupBy("RPM Description title").count().show()
pat_bp_df.where(col("Physician Name")=="Shariq Saghir").show()
pat_bp_df.where(col("Patient Name")=="Sharon Gordon").groupBy("Physician Name").count().show()
pat_bp_df.where(col("Status")=="Billable").show()
pat_bp_df.where(col("Status")=="Non billable").show()



# COMMAND ----------


#rpm recs
from pyspark.sql import SparkSession
from pyspark.sql.functions import col,month,substring


spark = SparkSession.builder.appName("phypatrpm").getOrCreate()
pat_rpm_df=spark.read.table("workspace.default.pat_rpm_rpm_records")
pat_rpm_df.distinct().show()
pat_rpm_df.groupBy("RPM Description title").count().show()

# COMMAND ----------

#physician patient records
from pyspark.sql import SparkSession
from pyspark.sql.functions import col,month,substring
spark = SparkSession.builder.appName("phypat").getOrCreate()
phy_df=spark.read.table("workspace.default.phy_pat_combined_report")
phy_df.show()
phy_df.groupBy("Patient Name").count().show()
phy_df.groupBy("Physician Name").count().show()

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col,month,substring
spark = SparkSession.builder.appName("salesorder").getOrCreate()
cust_order_df=spark.read.table("default.cust_order1")
prod_order_df=spark.read.table("default.prod_order")
prod_order_df.show()

#prod_order_df=prod_order_df.select(col("order_id"),col("product_id"),col("Order_Month"),col("Order_year"),col("qty"),col("date"), to_date(col("date"), "MM/dd/yy").alias("to_date"))
prod_order_df=prod_order_df.select(col("order_id"),col("product_id"),col("Order_Month"),col("Order_year"),col("qty"),col("date"),substring(col("date"),0,2).cast("int").alias("month"))
prod_order_df=prod_order_df.withColumn("prev_month", month("current_date")-1)
prod_order_df.show()
prod_order_df.select(col("order_id"),col("product_id"),col("Order_Month"),col("Order_year"),col("qty"),col("month"),col("prev_month")).where((col("month")==col("prev_month")) & (col("order_year")=="2025")).show()
prod_order_cnt_df=prod_order_df.groupBy("product_id").sum("qty")
prod_order_cnt_df=prod_order_cnt_df.withColumnRenamed("sum(qty)","sumqty")
#prod_order_cnt_df=prod_order_cnt_df.orderBy(desc("sumqty"))
prod_order_cnt_df.show()
#.where(col("month")==3).show()
#.where(col("month")==col("prev_month"))

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_date, expr,col,to_date,date_sub,substring
from pyspark.sql.functions import month,desc
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number,rank,dense_rank
windowSpec  = Window.orderBy(desc("sumqty"))
prod_order_cnt_df.show()
prod_order_cnt_df.withColumn("rank", rank().over(windowSpec)).show()

prod_order_cnt_df.withColumn("dense_rank", dense_rank().over(windowSpec)).show()










# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_date, expr,col,to_date,date_sub,substring
from pyspark.sql.functions import month,desc
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number,rank,dense_rank
prod_order_df.show()
windowSpec  = Window.partitionBy("product_id").orderBy(desc("qty"))
prod_order_df.alias('a').join(prod_order_cnt_df.alias('b'), col('a.product_id') == col('b.product_id')).select(col('a.product_id'),col('a.qty'),col('b.sumqty')).distinct().show()
#prod_order_cnt_df.withColumn("rank", rank().over(windowSpec)).show()

#prod_order_cnt_df.withColumn("dense_rank", dense_rank().over(windowSpec)).show()

# COMMAND ----------



# COMMAND ----------

# MAGIC %fs ls

# COMMAND ----------

def process_batch(batch_df, batch_id):
    # Perform any transformations or actions on the batch DataFrame
    batch_df.write.mode("append").saveAsTable("default.prod_order_out")
    batch_df.show()

streaming_df=spark.readStream.table("default.prod_order")
process_batch(prod_order_df, 0)
query = streaming_df.writeStream \
    .foreachBatch(process_batch) \
    .trigger(availableNow=True) \
    .option("checkpointLocation", "s3://your-bucket/checkpoints/prod_order") \
    .start()




   