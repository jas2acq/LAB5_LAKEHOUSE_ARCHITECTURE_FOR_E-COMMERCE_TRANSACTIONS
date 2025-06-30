from pyspark.context import SparkContext
from awsglue.context import GlueContext
from delta.tables import DeltaTable
import pyspark.sql.functions as F

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Enable Delta Lake extensions and configs (if not already set in Glue job)
spark.conf.set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
spark.conf.set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")

def process_table_delta(table_name, raw_path, processed_path):
    # Read raw CSV data
    raw_df = spark.read.format("csv").option("header", "true").load(raw_path)
    
    # Common transformations
    df = raw_df.withColumn("date", F.to_date("date"))
    
    if table_name == "orders":
        df = (df
              .withColumn("order_timestamp", F.to_timestamp("order_timestamp"))
              .dropDuplicates(["order_id"])
              # Keep order_num as requested
             )
    elif table_name == "order_items":
        df = (df
              .withColumn("order_timestamp", F.to_timestamp("order_timestamp"))
              .withColumn("days_since_prior_order", F.col("days_since_prior_order").cast("int"))
              .dropDuplicates(["id"])
             )
    elif table_name == "products":
        df = (df
              .withColumnRenamed("product_name", "name")
              .withColumn("department_id", F.col("department_id").cast("int"))
             )
    
    # Write as Delta table partitioned by date
    delta_path = f"{processed_path}/{table_name}"
    (df.write
       .format("delta")
       .mode("overwrite")
       .partitionBy("date")
       .option("overwriteSchema", "true")
       .save(delta_path)
    )
    
    # Optimize Delta table (auto compaction)
    delta_table = DeltaTable.forPath(spark, delta_path)
    delta_table.optimize().executeCompaction()

# Define your S3 paths here
raw_base_path = "s3://raw-bucket"
processed_base_path = "s3://processed-bucket/delta"

# Process each table
process_table_delta("orders", f"{raw_base_path}/orders/", processed_base_path)
process_table_delta("order_items", f"{raw_base_path}/order_items/", processed_base_path)
process_table_delta("products", f"{raw_base_path}/products/", processed_base_path)
