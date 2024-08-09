import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col
from awsglue.dynamicframe import DynamicFrame


"""
    Este código lee desde el AWS Glue Datacatalog dos tablas en una base de datos, después realiza un join entre ambas tablas y calcula el total de la venta. 
    Finalmente, escribe el resultado en un archivo Parquet en S3. 
"""

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

dyf_sales = glueContext.create_dynamic_frame.from_catalog(
    database="bddatalab", 
    table_name="sales_csv",
    transformation_ctx="dyf_sales"
)

dyf_products = glueContext.create_dynamic_frame.from_catalog(
    database="bddatalab",
    table_name="products_csv",
    transformation_ctx="dyf_products"
)

df_sales = dyf_sales.toDF()
df_products = dyf_products.toDF()
df_sales = df_sales.withColumn("quantity", col("quantity").cast("int"))
df_sales = df_sales.withColumn("price", col("price").cast("float"))
   
df_joined = df_sales.join(df_products, on="product_id", how="inner")

df_transformed = df_joined.withColumn("total_amount", col("quantity") * col("price"))

dyf_transformed = DynamicFrame.fromDF(df_transformed, glueContext, "dyf_transformed")

glueContext.write_dynamic_frame.from_options(
    frame = dyf_transformed,
    connection_type = "s3",
    connection_options = {"path": "s3://cloudcamp-target"},
    format = "parquet",
    transformation_ctx = "write_parquet"
)

job.commit()