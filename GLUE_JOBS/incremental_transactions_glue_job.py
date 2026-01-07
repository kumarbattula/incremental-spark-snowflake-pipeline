import sys
import json
import boto3
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql.functions import (
    input_file_name,
    current_timestamp,
    lit
)
import boto3
import json

# -----------------------------
# Glue Init
# -----------------------------
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# -----------------------------
# Read Snowflake creds from Secrets Manager
# -----------------------------
secret_name = "arn:aws:secretsmanager:us-east-1:920822857299:secret:snowflake-CD80sM"
region_name = "us-east-1"

client = boto3.client("secretsmanager", region_name=region_name)
secret = json.loads(client.get_secret_value(SecretId=secret_name)["SecretString"])

sf_options = {
    "sfURL": secret["sfURL"],
    "sfUser": secret["sfUser"],
    "sfPassword": secret["sfPassword"],
    "sfDatabase": "RETAIL_DB",
    "sfSchema": "AUDIT",
    "sfWarehouse": "COMPUTE_WH",
    "sfRole": "SYSADMIN"
}

# -----------------------------
# Read Audit Table
# -----------------------------
audit_df = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:snowflake://KYNPEVH-VW16779.snowflakecomputing.com") \
    .option("dbtable", "AUDIT.LOAD_AUDIT") \
    .option("user", secret["sfUser"]) \
    .option("password", secret["sfPassword"]) \
    .option("warehouse", "COMPUTE_WH") \
    .option("database", "RETAIL_DB") \
    .option("schema", "AUDIT") \
    .option("driver", "net.snowflake.client.jdbc.SnowflakeDriver") \
    .load()

processed_files = (
    audit_df
    .select("file_name")
    .distinct()
    .rdd
    .map(lambda x: x[0])
    .collect()
)

# -----------------------------
# Read Files from S3
# -----------------------------
raw_df = spark.read \
    .option("header", "true") \
    .csv("s3://bks-data-incre/transactions/") \
    .withColumn("file_name", input_file_name())

# -----------------------------
# Incremental Logic
# -----------------------------
if processed_files:
    new_df = raw_df.filter(~raw_df.file_name.isin(processed_files))
else:
    new_df = raw_df

# -----------------------------
# Exit if no new data
# -----------------------------
if new_df.rdd.isEmpty():
    print("No new files to process")
    job.commit()
    sys.exit(0)

# -----------------------------
# Transform
# -----------------------------
final_df = new_df.select(
    "txn_id",
    "customer_id",
    "amount",
    "txn_date"
).withColumn("load_ts", current_timestamp())

# -----------------------------
# Write to Snowflake Target Table
# -----------------------------
final_df.write \
    .format("jdbc") \
    .option("url", "jdbc:snowflake://KYNPEVH-VW16779.snowflakecomputing.com") \
    .option("dbtable", "PUBLIC.TRANSACTIONS") \
    .option("user", secret["sfUser"]) \
    .option("password", secret["sfPassword"]) \
    .option("warehouse", "COMPUTE_WH") \
    .option("database", "RETAIL_DB") \
    .option("schema", "PUBLIC") \
    .option("driver", "net.snowflake.client.jdbc.SnowflakeDriver") \
    .mode("append") \
    .save()

# -----------------------------
# Write Audit Records
# -----------------------------
audit_insert_df = new_df.groupBy("file_name") \
    .count() \
    .withColumnRenamed("count", "record_count") \
    .withColumn("status", lit("SUCCESS")) \
    .withColumn("load_ts", current_timestamp())

audit_insert_df.write \
    .format("jdbc") \
    .option("url", "jdbc:snowflake://KYNPEVH-VW16779.snowflakecomputing.com") \
    .option("dbtable", "AUDIT.LOAD_AUDIT") \
    .option("user", secret["sfUser"]) \
    .option("password", secret["sfPassword"]) \
    .option("warehouse", "COMPUTE_WH") \
    .option("database", "RETAIL_DB") \
    .option("schema", "AUDIT") \
    .option("driver", "net.snowflake.client.jdbc.SnowflakeDriver") \
    .mode("append") \
    .save()


job.commit()
