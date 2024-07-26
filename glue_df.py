import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col
from awsglue.dynamicframe import DynamicFrame

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

df_sales = spark.read.format("csv").option("header", "true").load("s3://path-to-file/sales.csv")
df_products = spark.read.format("csv").option("header", "true").load("s3://path-to-file/products.csv")

df_sales = df_sales.withColumn("quantity", col("quantity").cast("int"))
df_sales = df_sales.withColumn("price", col("price").cast("float"))
   
df_joined = df_sales.join(df_products, on="product_id", how="inner")

df_transformed = df_joined.withColumn("total_amount", col("quantity") * col("price"))

df_transformed.write.mode("overwrite").parquet("s3://path-to-target")

job.commit()