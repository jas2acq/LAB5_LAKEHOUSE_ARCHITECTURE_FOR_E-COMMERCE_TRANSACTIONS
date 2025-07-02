from datetime import datetime
import json
import logging
import sys

import boto3
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from delta.tables import DeltaTable
from pyspark.context import SparkContext
from pyspark.sql import Window
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, LongType, TimestampType, DecimalType, DateType, StringType


class S3LogHandler:
    """Custom logging handler for writing logs to S3 in AWS Glue jobs."""

    def __init__(self, bucket: str, log_key_prefix: str, job_name: str):
        """Initialize the S3 logging handler.

        Args:
            bucket: S3 bucket name for storing logs.
            log_key_prefix: Prefix for the S3 log file path.
            job_name: Name of the AWS Glue job.
        """
        self.bucket = bucket
        self.log_key_prefix = log_key_prefix
        self.job_name = job_name
        self.s3 = boto3.client("s3")
        self.log_buffer = []
        self.job_start_time = datetime.utcnow().strftime("%Y-%m-%d_%H-%M-%S")
        self.log_file_key = f"{log_key_prefix}/{job_name}_{self.job_start_time}.log"
        self.rejected_records_base_path = f"s3://{self.bucket}/rejected_records"

    def log(self, level: str, message: str):
        """Add a log entry to the buffer and print to CloudWatch.

        Args:
            level: Log level (e.g., INFO, ERROR, WARNING).
            message: Log message to record.
        """
        timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        log_entry = f"{timestamp} {level}: {message}"
        self.log_buffer.append(log_entry)
        print(log_entry)

        # Upload logs periodically to avoid memory issues
        if len(self.log_buffer) >= 50:
            self.upload_logs(append=True)

    def info(self, message: str):
        """Log an INFO-level message."""
        self.log(level="INFO", message=message)

    def error(self, message: str):
        """Log an ERROR-level message."""
        self.log(level="ERROR", message=message)

    def warning(self, message: str):
        """Log a WARNING-level message."""
        self.log(level="WARNING", message=message)

    def upload_logs(self, append: bool = True):
        """Upload buffered logs to S3.

        Args:
            append: If True, append to existing log file; otherwise, overwrite.
        """
        if not self.log_buffer:
            return

        try:
            existing_content = ""
            if append:
                try:
                    obj = self.s3.get_object(Bucket=self.bucket, Key=self.log_file_key)
                    existing_content = obj["Body"].read().decode("utf-8") + "\n"
                except self.s3.exceptions.NoSuchKey:
                    pass  # File doesn't exist yet

            new_content = existing_content + "\n".join(self.log_buffer)
            self.s3.put_object(
                Bucket=self.bucket,
                Key=self.log_file_key,
                Body=new_content.encode("utf-8"),
                ContentType="text/plain",
            )
            print(f"Uploaded logs to s3://{self.bucket}/{self.log_file_key}")
            self.log_buffer.clear()

        except Exception as e:
            print(f"Failed to upload logs to S3: {e}")


class IncrementalDeltaProcessor:
    """Enhanced Delta Lake processor with incremental processing and S3 logging."""

    def __init__(self, spark_session, bucket_name: str = "lab5-cicd", job_name: str = "transform_job"):
        """Initialize the Delta Lake processor.

        Args:
            spark_session: SparkSession object for DataFrame operations.
            bucket_name: S3 bucket name for data and logs (default: 'lab5-cicd').
            job_name: Name of the AWS Glue job (default: 'transform_job').
        """
        self.spark = spark_session
        self.bucket_name = bucket_name
        self.job_name = job_name
        self.raw_base_path = f"s3://{bucket_name}/raw"
        self.processed_base_path = f"s3://{bucket_name}/processed"
        self.state_path = f"s3://{bucket_name}/state"
        self.s3_client = boto3.client("s3")
        self.s3_logger = S3LogHandler(bucket_name, "logs", job_name)
        self.processed_registry_uri = f"s3://{bucket_name}/state/transform_processed_files.json"

        # Configure Delta Lake optimizations
        self._configure_delta_optimizations()

        # Setup standard logging
        logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
        self.logger = logging.getLogger(__name__)

        # Table configurations
        self.table_configs = {
            "orders": {
                "primary_key": "order_id",
                "partition_columns": ["date"],
                "schema": self._get_orders_schema(),
                "transformations": self._transform_orders,
            },
            "order_items": {
                "primary_key": "id",
                "partition_columns": ["date"],
                "schema": self._get_order_items_schema(),
                "transformations": self._transform_order_items,
            },
            "products": {
                "primary_key": "product_id",
                "partition_columns": ["department"],
                "schema": self._get_products_schema(),
                "transformations": self._transform_products,
            },
        }

    def _configure_delta_optimizations(self):
        """Configure Spark for optimal Delta Lake performance."""
        configs = {
            "spark.databricks.delta.optimizeWrite.enabled": "true",
            "spark.databricks.delta.autoCompact.enabled": "true",
            "spark.databricks.delta.retentionDurationCheck.enabled": "false",
            "spark.sql.adaptive.enabled": "true",
            "spark.sql.adaptive.coalescePartitions.enabled": "true",
        }
        for key, value in configs.items():
            self.spark.conf.set(key, value)

    def _get_orders_schema(self) -> StructType:
        """Define schema for the orders table."""
        return StructType([
            StructField("order_num", IntegerType(), True),
            StructField("order_id", LongType(), False),
            StructField("user_id", LongType(), False),
            StructField("order_timestamp", TimestampType(), True),
            StructField("total_amount", DecimalType(10, 2), True),
            StructField("date", DateType(), False),
        ])

    def _get_order_items_schema(self) -> StructType:
        """Define schema for the order_items table."""
        return StructType([
            StructField("id", LongType(), False),
            StructField("order_id", LongType(), False),
            StructField("user_id", LongType(), False),
            StructField("days_since_prior_order", IntegerType(), True),
            StructField("product_id", LongType(), False),
            StructField("add_to_cart_order", IntegerType(), True),
            StructField("reordered", IntegerType(), True),
            StructField("order_timestamp", TimestampType(), True),
            StructField("date", DateType(), False),
        ])

    def _get_products_schema(self) -> StructType:
        """Define schema for the products table."""
        return StructType([
            StructField("product_id", LongType(), False),
            StructField("department_id", IntegerType(), True),
            StructField("department", StringType(), True),
            StructField("name", StringType(), True),
        ])

    def _transform_orders(self, df):
        """Apply transformations specific to the orders table."""
        return (df
                .withColumn("order_id", F.col("order_id").cast(LongType()))
                .withColumn("user_id", F.col("user_id").cast(LongType()))
                .withColumn("order_num", F.col("order_num").cast(IntegerType()))
                .withColumn("order_timestamp", F.to_timestamp("order_timestamp"))
                .withColumn("total_amount", F.col("total_amount").cast(DecimalType(10, 2)))
                .withColumn("date", F.to_date("date"))
                .withColumn("year", F.year("date"))
                .withColumn("month", F.month("date"))
                .withColumn("day_of_week", F.dayofweek("date")))

    def _transform_order_items(self, df):
        """Apply transformations specific to the order_items table."""
        return (df
                .withColumn("id", F.col("id").cast(LongType()))
                .withColumn("order_id", F.col("order_id").cast(LongType()))
                .withColumn("user_id", F.col("user_id").cast(LongType()))
                .withColumn("product_id", F.col("product_id").cast(LongType()))
                .withColumn("days_since_prior_order", F.col("days_since_prior_order").cast(IntegerType()))
                .withColumn("add_to_cart_order", F.col("add_to_cart_order").cast(IntegerType()))
                .withColumn("reordered", F.col("reordered").cast(IntegerType()))
                .withColumn("order_timestamp", F.to_timestamp("order_timestamp"))
                .withColumn("date", F.to_date("date"))
                .withColumn("is_reorder", F.when(F.col("reordered") == 1, True).otherwise(False)))

    def _transform_products(self, df):
        """Apply transformations specific to the products table."""
        return (df
                .withColumn("product_id", F.col("product_id").cast(LongType()))
                .withColumn("department_id", F.col("department_id").cast(IntegerType()))
                .withColumnRenamed("product_name", "name")
                .withColumn("name_length", F.length("name"))
                .withColumn("created_date", F.current_date()))

    def _get_file_modification_time(self, bucket: str, key: str):
        """Get the modification time of an S3 file.

        Args:
            bucket: S3 bucket name.
            key: S3 object key.

        Returns:
            datetime: Last modified time, or None if retrieval fails.
        """
        try:
            response = self.s3_client.head_object(Bucket=bucket, Key=key)
            return response["LastModified"]
        except Exception as e:
            self.s3_logger.error(f"Failed to get modification time for {key}: {e}")
            return None

    def _load_processed_registry(self) -> dict:
        """Load the processed files registry from S3.

        Returns:
            dict: Registry of processed files, or empty dict if not found or error occurs.
        """
        try:
            obj = self.s3_client.get_object(Bucket=self.bucket_name, Key=self.processed_registry_uri.split("/", 3)[-1])
            return json.loads(obj["Body"].read().decode("utf-8"))
        except self.s3_client.exceptions.NoSuchKey:
            return {}
        except Exception as e:
            self.s3_logger.error(f"Error loading processed registry: {e}")
            return {}

    def _save_processed_registry(self, registry: dict):
        """Save the processed files registry to S3.

        Args:
            registry: Dictionary containing processed file metadata.
        """
        try:
            key = self.processed_registry_uri.split("/", 3)[-1]
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=key,
                Body=json.dumps(registry, indent=2).encode("utf-8"),
                ContentType="application/json",
            )
        except Exception as e:
            self.s3_logger.error(f"Error saving processed registry: {e}")

    def _should_process_file(self, table_name: str, processed_registry: dict) -> bool:
        """Check if a file should be processed based on its modification time.

        Args:
            table_name: Name of the table to check.
            processed_registry: Dictionary of previously processed files.

        Returns:
            bool: True if the file should be processed, False otherwise.
        """
        raw_key = f"raw/{table_name}/combined.csv"
        try:
            self.s3_client.head_object(Bucket=self.bucket_name, Key=raw_key)
        except self.s3_client.exceptions.NoSuchKey:
            self.s3_logger.info(f"No raw data file found for {table_name}")
            return False

        current_mod_time = self._get_file_modification_time(self.bucket_name, raw_key)
        if not current_mod_time:
            return False

        if table_name in processed_registry:
            last_processed_time = datetime.fromisoformat(processed_registry[table_name]["last_processed"])
            if current_mod_time.replace(tzinfo=None) <= last_processed_time:
                self.s3_logger.info(f"File {table_name}/combined.csv hasn't been modified since last processing")
                return False

        return True

    def _check_data_quality(self, df, table_name: str, config: dict):
        """Perform data quality checks, filter out bad records, and return DataFrames with metrics.

        Args:
            df: Input Spark DataFrame to check.
            table_name: Name of the table being processed.
            config: Configuration dictionary with primary_key and other settings.

        Returns:
            tuple: (good_df, rejected_df, quality_metrics)
        """
        primary_key = config["primary_key"]

        # Null Primary Key Check
        null_pk_records = df.filter(F.col(primary_key).isNull())
        good_df = df.filter(F.col(primary_key).isNotNull())

        # Add rejection reason to null primary key records
        if null_pk_records.count() > 0:
            null_pk_records = null_pk_records.withColumn("rejection_reason", F.lit(f"Null primary key: {primary_key}"))

        # Duplicate Primary Key Check
        window_spec = Window.partitionBy(primary_key).orderBy(F.current_timestamp())
        df_with_row_number = good_df.withColumn("rn", F.row_number().over(window_spec))
        duplicate_pk_records = df_with_row_number.filter(F.col("rn") > 1).drop("rn")
        good_df = df_with_row_number.filter(F.col("rn") == 1).drop("rn")

        if duplicate_pk_records.count() > 0:
            duplicate_pk_records = duplicate_pk_records.withColumn(
                "rejection_reason", F.lit(f"Duplicate primary key: {primary_key}")
            )

        # Combine rejected records
        rejected_df = null_pk_records.unionByName(duplicate_pk_records, allowMissingColumns=True)

        # Null Date Records Check (for logging, not rejection)
        null_date_count = good_df.filter(F.col("date").isNull()).count() if "date" in good_df.columns else 0

        # Compute quality metrics
        quality_metrics = {
            "total_records": df.count(),
            "good_records_count": good_df.count(),
            "rejected_records_count": rejected_df.count(),
            "null_primary_key_count": null_pk_records.count(),
            "duplicate_primary_key_count": duplicate_pk_records.count(),
            "null_date_records_count": null_date_count,
        }

        # Log quality metrics
        self.s3_logger.info(f"Data Quality Metrics for {table_name}:")
        for metric, value in quality_metrics.items():
            self.s3_logger.info(f"  {metric}: {value}")

        # Warn if no records are found
        if quality_metrics["total_records"] == 0:
            self.s3_logger.warning(f"No records found for table {table_name}")

        return good_df, rejected_df, quality_metrics

    def _write_rejected_records(self, df, table_name: str):
        """Write rejected records to S3 as Parquet files.

        Args:
            df: Spark DataFrame containing rejected records.
            table_name: Name of the table being processed.
        """
        if df.count() == 0:
            self.s3_logger.info(f"No rejected records to write for {table_name}.")
            return

        rejected_path = f"{self.s3_logger.rejected_records_base_path}/{table_name}/rejected_batch_{datetime.utcnow().strftime('%Y%m%d%H%M%S')}"
        try:
            df_to_write = df.withColumn("rejection_timestamp", F.current_timestamp())
            if "rejection_reason" not in df_to_write.columns:
                df_to_write = df_to_write.withColumn("rejection_reason", F.lit("Unspecified rejection reason"))
            else:
                df_to_write = df_to_write.withColumn("rejection_reason", F.col("rejection_reason").cast(StringType()))

            (df_to_write.write
             .format("parquet")
             .mode("append")
             .save(rejected_path))
            self.s3_logger.warning(f"Rejected records for {table_name} written to {rejected_path}")

        except Exception as e:
            self.s3_logger.error(f"Failed to write rejected records for {table_name}: {e}")

    def _write_quality_metrics(self, table_name: str, metrics: dict):
        """Write quality metrics to S3 as a JSON file.

        Args:
            table_name: Name of the table being processed.
            metrics: Dictionary of quality metrics.
        """
        metrics_with_timestamp = {
            "table_name": table_name,
            "timestamp": datetime.utcnow().isoformat(),
            "metrics": metrics,
        }
        metrics_key = f"state/quality_metrics/{table_name}_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.json"
        try:
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=metrics_key,
                Body=json.dumps(metrics_with_timestamp, indent=2),
                ContentType="application/json",
            )
            self.s3_logger.info(f"Quality metrics written to s3://{self.bucket_name}/{metrics_key}")
        except Exception as e:
            self.s3_logger.error(f"Failed to write quality metrics: {e}")

    def process_table(self, table_name: str, mode: str = "merge") -> bool:
        """Process a single table with incremental processing and quality checks.

        Args:
            table_name: Name of the table to process.
            mode: Write mode ('merge', 'append', or 'overwrite'; default: 'merge').

        Returns:
            bool: True if processing succeeds, False if no data or no good records.
        """
        try:
            self.s3_logger.info(f"Starting processing for table: {table_name}")

            if table_name not in self.table_configs:
                raise ValueError(f"Unknown table: {table_name}")

            config = self.table_configs[table_name]
            raw_path = f"{self.raw_base_path}/{table_name}/combined.csv"
            processed_path = f"{self.processed_base_path}/{table_name}"

            # Read raw data
            try:
                raw_df = (self.spark.read
                          .format("csv")
                          .option("header", "true")
                          .option("inferSchema", "false")
                          .load(raw_path))
                if raw_df.count() == 0:
                    self.s3_logger.warning(f"No data found at {raw_path}")
                    return False
            except Exception as e:
                self.s3_logger.error(f"Failed to read raw data from {raw_path}: {e}")
                return False

            # Apply transformations
            transformed_df = config["transformations"](raw_df)

            # Data quality checks and segregation
            good_df, rejected_df, quality_metrics = self._check_data_quality(transformed_df, table_name, config)
            self._write_quality_metrics(table_name, quality_metrics)
            self._write_rejected_records(rejected_df, table_name)

            # Check if there are good records to process
            if good_df.count() == 0:
                self.s3_logger.warning(f"No good records to process for table {table_name} after quality checks.")
                return False

            # Write good data based on mode
            if mode == "overwrite":
                self._write_overwrite(good_df, processed_path, config)
            elif mode == "append":
                self._write_append(good_df, processed_path, config)
            elif mode == "merge":
                self._write_merge(good_df, processed_path, config, table_name)
            else:
                raise ValueError(f"Unsupported write mode: {mode}")

            # Optimize table
            self._optimize_table(processed_path)

            self.s3_logger.info(f"Successfully processed {quality_metrics['good_records_count']} good records for {table_name}")
            return True

        except Exception as e:
            self.s3_logger.error(f"Error processing table {table_name}: {e}")
            raise

    def _write_overwrite(self, df, path: str, config: dict):
        """Write data to S3 in overwrite mode.

        Args:
            df: Spark DataFrame to write.
            path: S3 path to write to.
            config: Table configuration with partition columns.
        """
        (df.write
         .format("delta")
         .mode("overwrite")
         .partitionBy(*config["partition_columns"])
         .option("overwriteSchema", "true")
         .save(path))

    def _write_append(self, df, path: str, config: dict):
        """Write data to S3 in append mode.

        Args:
            df: Spark DataFrame to write.
            path: S3 path to write to.
            config: Table configuration with partition columns.
        """
        (df.write
         .format("delta")
         .mode("append")
         .partitionBy(*config["partition_columns"])
         .save(path))

    def _write_merge(self, df, path: str, config: dict, table_name: str):
        """Write data to S3 using Delta merge (upsert).

        Args:
            df: Spark DataFrame to merge.
            path: S3 path to the Delta table.
            config: Table configuration with primary_key.
            table_name: Name of the table.
        """
        primary_key = config["primary_key"]
        if not self._table_exists(path):
            self._write_overwrite(df, path, config)
            return

        delta_table = DeltaTable.forPath(self.spark, path)
        (delta_table.alias("target")
         .merge(df.alias("source"), f"target.{primary_key} = source.{primary_key}")
         .whenMatchedUpdateAll()
         .whenNotMatchedInsertAll()
         .execute())
        self.s3_logger.info(f"Completed merge operation for {table_name}")

    def _table_exists(self, path: str) -> bool:
        """Check if a Delta table exists at the given path.

        Args:
            path: S3 path to the Delta table.

        Returns:
            bool: True if the table exists, False otherwise.
        """
        try:
            DeltaTable.forPath(self.spark, path)
            return True
        except:
            return False

    def _optimize_table(self, path: str):
        """Optimize a Delta table with compaction.

        Args:
            path: S3 path to the Delta table.
        """
        try:
            delta_table = DeltaTable.forPath(self.spark, path)
            delta_table.optimize().executeCompaction()
            self.s3_logger.info(f"Optimized Delta table at {path}")
        except Exception as e:
            self.s3_logger.warning(f"Failed to optimize table at {path}: {e}")

    def process_all_tables(self, mode: str = "merge") -> dict:
        """Process all configured tables with incremental processing and error isolation.

        Args:
            mode: Write mode ('merge', 'append', or 'overwrite'; default: 'merge').

        Returns:
            dict: Dictionary mapping table names to processing results ('skipped', True, or False).
        """
        self.s3_logger.info("Starting incremental Delta Lake processing job")
        processed_registry = self._load_processed_registry()
        results = {}
        processed_count = 0
        skipped_count = 0

        for table_name in self.table_configs.keys():
            try:
                if not self._should_process_file(table_name, processed_registry):
                    self.s3_logger.info(f"Skipping {table_name} - no new data to process")
                    results[table_name] = "skipped"
                    skipped_count += 1
                    continue

                success = self.process_table(table_name, mode)
                results[table_name] = success

                if success:
                    processed_count += 1
                    processed_registry[table_name] = {
                        "last_processed": datetime.utcnow().isoformat(),
                        "status": "completed",
                    }

            except Exception as e:
                self.s3_logger.error(f"Failed to process {table_name}: {e}")
                results[table_name] = False

        self._save_processed_registry(processed_registry)
        total = len(self.table_configs)
        failed_count = sum(1 for result in results.values() if result is False)
        self.s3_logger.info(f"Processing complete: {processed_count} processed, {skipped_count} skipped, {failed_count} failed out of {total} total tables")

        if failed_count > 0:
            failed_tables = [table for table, result in results.items() if result is False]
            self.s3_logger.error(f"Failed tables: {failed_tables}")

        self.s3_logger.upload_logs(append=True)
        return results


def main():
    """Main execution function for the AWS Glue job."""
    sc = SparkContext()
    glue_context = GlueContext(sc)
    spark = glue_context.spark_session
    args = getResolvedOptions(sys.argv, ["JOB_NAME"])
    job = Job(glue_context)
    job.init(args["JOB_NAME"], args)

    try:
        processor = IncrementalDeltaProcessor(spark, job_name=args["JOB_NAME"])
        results = processor.process_all_tables(mode="merge")
        failed_results = [table for table, result in results.items() if result is False]
        if failed_results:
            raise Exception(f"Failed to process tables: {failed_results}")
        processor.s3_logger.info("All tables processed successfully")

    except Exception as e:
        print(f"Job failed: {e}")
        raise
    finally:
        job.commit()


if __name__ == "__main__":
    main()
