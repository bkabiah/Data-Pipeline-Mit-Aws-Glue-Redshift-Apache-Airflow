import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, to_date

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'S3_BUCKET', 'REDSHIFT_DB', 'REDSHIFT_USER', 'REDSHIFT_PASSWORD', 'REDSHIFT_URL'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# 1. S3 JSON (Orders) lesen
orders_df = spark.read.json(f"s3://{args['S3_BUCKET']}/orders/")

# 2. S3 JSON (Customers from Lambda/Postgres) lesen
customers_df = spark.read.json(f"s3://{args['S3_BUCKET']}/postgres_customers/")

# 3. Transformieren & Joinen
orders_clean = orders_df.withColumn("order_date", to_date(col("timestamp"))) \
                        .select("order_id", "customer_id", "order_date", "total_amount", "status")

joined_df = orders_clean.join(customers_df, "customer_id", "left") \
                        .withColumn("source_system", col("source").otherwise("webshop"))

# 4. Nach Redshift schreiben
redshift_url = args['REDSHIFT_URL']
redshift_db = args['REDSHIFT_DB']

joined_df.write \
    .format("com.databricks.spark.redshift") \
    .option("url", f"jdbc:redshift://{redshift_url}/{redshift_db}?user={args['REDSHIFT_USER']}&password={args['REDSHIFT_PASSWORD']}") \
    .option("dbtable", "ecommerce.fact_customer_orders") \
    .option("tempdir", f"s3://{args['S3_BUCKET']}/temp/") \
    .option("aws_iam_role", "arn:aws:iam::[DEINE_ACCOUNT_ID]:role/GlueRedshiftRole") \
    .mode("append") \
    .save()

job.commit()
