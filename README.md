# Laboratorio AWS Glue
## Arquitectura
![Arquitectura del Laboratorio.](https://github.com/cloudcampla/aws-glue-101/blob/main/architecture.png?raw=true)

## **Objetivo**
* Construir un ETL (Extract, Transform, Load) que utilice DynamicFrames en AWS Glue.

## **Descripción**
* Este código lee desde el AWS Glue Datacatalog dos tablas en una base de datos, después realiza un join entre ambas tablas y calcula el total de la venta. Finalmente, escribe el resultado en un archivo Parquet en S3. 

## **Qué voy a Aprender?** 
Al final del laboratorio el estudiante aprenderá:
* Como construir un ETL en AWS Glue y toda la configuración requerida para lograrlo.
* Como utilizar los rastreadores de AWS Glue para identificar estructuras de datos y alimentar el AWS Glue Data Catalog.
* Como leer información del AWS Glue Data Catalog y utilizarla para hacer las transformaciones necesarias.
* Como convertir archivos de CSV a Parquet.
* Como Cargar información transformada a Amazon S3.

## **Servicios de AWS a Utilizar**
* [Amazon Athena](https://aws.amazon.com/athena/).
* [AWS Glue](https://aws.amazon.com/glue/).
* [AWS Glue Data Catalog](https://docs.aws.amazon.com/es_es/glue/latest/dg/start-data-catalog.html).
* [AWS Glue Crawler](https://docs.aws.amazon.com/glue/latest/dg/add-crawler.html).
* [Amazon S3](https://aws.amazon.com/s3/).
* [IAM](https://aws.amazon.com/iam/).

## **Conceptos**
* **Dynamic Frame:** Es una estructura de datos similar a un DataFrame de Apache Spark, pero con características adicionales que lo hacen más adecuado para los procesos de transformación de datos en AWS Glue. 
* **Data Frame:** Es una estructura de datos similar a un DataFrame de Apache Spark, pero con características adicionales que lo hacen más adecuado para los procesos de transformación de datos en AWS Glue.
* **Inner Join:** es un tipo de operación de combinación (join) en bases de datos y sistemas de procesamiento de datos (como SQL, Spark, etc.) que se utiliza para combinar filas de dos tablas basándose en una condición común.

## **Explicación del Código**
A continuación voy a explicar detalladamente el código del archivo glue_dyf.py

```
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
```
Lee datos desde el AWS Glue Data Catalog, sales_csv y products_csv son leidos de la base de datos bddatalab. Estos set de datos son cargados en Dynamic Frames.

```
df_sales = dyf_sales.toDF()
df_products = dyf_products.toDF()
```
Los Dynamic Frames se convierten en DataFrames para ser manipulados fácilmente. 

```
df_sales = df_sales.withColumn("quantity", col("quantity").cast("int"))
df_sales = df_sales.withColumn("price", col("price").cast("float"))
```
Se hacen las siguientes transformaciones, la columna "quiantity" se convierte en un entero. y La columna "price" es convertido en un flotante. 

```
df_joined = df_sales.join(df_products, on="product_id", how="inner")
```
Los DataFrames df_sales y df_products se unen según a través de la columna product_id mediante un inner join.

```
df_transformed = df_joined.withColumn("total_amount", col("quantity") * col("price"))
```
Se agrega una nueva columna al DataFrame llamada "total_amount" multiplicando el valor de "quantity" y "price".

```
dyf_transformed = DynamicFrame.fromDF(df_transformed, glueContext, "dyf_transformed")
```
El DataFrame es convertido nuevamente en un DynamicFrame para ser escrito en Amazon S3.

```
glueContext.write_dynamic_frame.from_options(
    frame = dyf_transformed,
    connection_type = "s3",
    connection_options = {"path": "s3://cloudcamp-target"},
    format = "parquet",
    transformation_ctx = "write_parquet"
)
```
El Dynamic Frame es escrito en Amazon S3 en el bucket cloudcamp-target en formato parquet.

```
job.commit()
```
Se escriben todos los cambios hechos de forma satisfactoria.




## Video del Laboratorio





