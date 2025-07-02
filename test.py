import pandas as pd
import os
import logging
import json
from openpyxl.utils.exceptions import InvalidFileException

def process_monthly_excel_file_with_sheet_tracking(
    excel_path,
    processed_registry_path='processed_files.json',
    processed_ids_dir='processed_ids',
    size_threshold_mb=10
):
    """
    Processes monthly Excel files with deduplication and per-sheet tracking.
    Tracks processed sheets per file to avoid reprocessing sheets on file re-runs.
    """

    if not logging.getLogger().hasHandlers():
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )

    EXPECTED_COLUMNS = {
        'orders': {'order_num', 'order_id', 'user_id', 'order_timestamp', 'total_amount', 'date'},
        'order_items': {'id', 'order_id', 'user_id', 'days_since_prior_order', 'product_id', 'add_to_cart_order', 'reordered', 'order_timestamp', 'date'},
        'products': {'product_id', 'department_id', 'department', 'product_name'}
    }

    UNIQUE_KEYS = {
        'orders': 'order_id',
        'order_items': 'id',
        'products': 'product_id'
    }

    os.makedirs(processed_ids_dir, exist_ok=True)

    def load_processed_registry():
        if os.path.exists(processed_registry_path):
            try:
                with open(processed_registry_path, 'r') as f:
                    return json.load(f)  # dict: {filename: [sheet1, sheet2, ...]}
            except Exception as e:
                logging.warning(f"Could not load processed registry file: {e}")
                return {}
        return {}

    def save_processed_registry(registry):
        try:
            with open(processed_registry_path, 'w') as f:
                json.dump(registry, f, indent=2)
        except Exception as e:
            logging.error(f"Failed to save processed registry: {e}")

    def load_processed_ids(data_type):
        path = os.path.join(processed_ids_dir, f'{data_type}_ids.json')
        if os.path.exists(path):
            try:
                with open(path, 'r') as f:
                    return set(json.load(f))
            except Exception as e:
                logging.warning(f"Could not load processed IDs for {data_type}: {e}")
                return set()
        return set()

    def save_processed_ids(data_type, ids_set):
        path = os.path.join(processed_ids_dir, f'{data_type}_ids.json')
        try:
            with open(path, 'w') as f:
                json.dump(list(ids_set), f)
        except Exception as e:
            logging.error(f"Failed to save processed IDs for {data_type}: {e}")

    def columns_match(expected_cols, df_cols):
        return expected_cols == set(df_cols)

    def identify_sheet_type(df):
        for data_type, cols in EXPECTED_COLUMNS.items():
            if columns_match(cols, df.columns):
                return data_type
        return None

    def append_to_csv(df, data_type):
        csv_file = f'{data_type}_combined.csv'
        header = not os.path.exists(csv_file)
        try:
            df.to_csv(csv_file, mode='a', index=False, header=header)
            logging.info(f"Appended {len(df)} new rows to {csv_file}")
        except Exception as e:
            logging.error(f"Failed to append data to {csv_file}: {e}")

    processed_registry = load_processed_registry()
    filename = os.path.basename(excel_path)

    # Get list of sheets already processed for this file (empty list if none)
    processed_sheets_for_file = set(processed_registry.get(filename, []))

    try:
        if not os.path.exists(excel_path):
            raise FileNotFoundError(f"File not found: {excel_path}")

        file_size_mb = os.path.getsize(excel_path) / (1024 * 1024)
        logging.info(f"Processing file '{excel_path}' ({file_size_mb:.2f} MB)")

        xls = pd.ExcelFile(excel_path, engine='openpyxl')
        sheet_names = xls.sheet_names

        if not sheet_names:
            raise ValueError("Excel file contains no sheets")

        # Load processed IDs for all data types once
        processed_ids = {dt: load_processed_ids(dt) for dt in EXPECTED_COLUMNS.keys()}

        # Filter sheets to process: only those NOT in processed_sheets_for_file
        sheets_to_process = [s for s in sheet_names if s not in processed_sheets_for_file]

        if not sheets_to_process:
            logging.info(f"All sheets in file '{filename}' have already been processed. Skipping.")
            return

        # Process sheets one by one (chunking) regardless of file size for simplicity
        for i, sheet in enumerate(sheets_to_process, start=1):
            logging.info(f"Processing sheet {i}/{len(sheets_to_process)}: '{sheet}'")
            try:
                df = pd.read_excel(xls, sheet_name=sheet, engine='openpyxl')

                if df.empty:
                    logging.warning(f"Sheet '{sheet}' is empty, skipping.")
                    # Mark empty sheet as processed to avoid reprocessing
                    processed_sheets_for_file.add(sheet)
                    continue

                sheet_type = identify_sheet_type(df)

                if sheet_type:
                    key_col = UNIQUE_KEYS[sheet_type]
                    before_count = len(df)
                    df = df[~df[key_col].isin(processed_ids[sheet_type])]
                    after_count = len(df)

                    if after_count == 0:
                        logging.info(f"All rows in sheet '{sheet}' are duplicates, skipping append.")
                    else:
                        append_to_csv(df, sheet_type)
                        processed_ids[sheet_type].update(df[key_col].astype(str).tolist())
                        logging.info(f"Filtered {before_count - after_count} duplicate rows in sheet '{sheet}'")

                    # Mark sheet as processed
                    processed_sheets_for_file.add(sheet)

                else:
                    logging.warning(f"Sheet '{sheet}' columns do not match any expected schema. Ignoring.")
                    # Still mark as processed to avoid repeated attempts
                    processed_sheets_for_file.add(sheet)

            except Exception as e:
                logging.error(f"Error reading sheet '{sheet}': {e}")

        # Save updated processed IDs
        for dt, ids_set in processed_ids.items():
            save_processed_ids(dt, ids_set)

        # Update registry with new processed sheets for this file
        processed_registry[filename] = list(processed_sheets_for_file)
        save_processed_registry(processed_registry)

        logging.info(f"Finished processing file '{filename}'")

    except FileNotFoundError as e:
        logging.error(e)
    except PermissionError as e:
        logging.error(f"Permission denied: {e}")
    except InvalidFileException as e:
        logging.error(f"Invalid or corrupted Excel file: {e}")
    except ValueError as e:
        logging.error(f"Value error: {e}")
    except MemoryError:
        logging.error("Memory error: File too large to process.")
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")




process_monthly_excel_file_with_sheet_tracking(r'C:\Users\jason\OneDrive\Desktop\p2-fin\L5\LAB5_LAKEHOUSE_ARCHITECTURE_FOR_E-COMMERCE_TRANSACTIONS\data\raw\order_items_apr_2025.xlsx', size_threshold_mb=10)

"""
Enhanced Delta Lake Data Processor for AWS Glue with S3 Logging and Incremental Processing
"""

from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from delta.tables import DeltaTable
import pyspark.sql.functions as F
from pyspark.sql.types import *
import sys
import logging
from datetime import datetime
import boto3
import json

class S3LogHandler:
    """Custom S3 logging handler for Glue jobs"""
    
    def __init__(self, bucket, log_key_prefix, job_name):
        self.bucket = bucket
        self.log_key_prefix = log_key_prefix
        self.s3 = boto3.client('s3')
        self.log_buffer = []
        self.job_name = job_name
        self.job_start_time = datetime.utcnow().strftime('%Y-%m-%d_%H-%M-%S')
        self.log_file_key = f"{log_key_prefix}/{job_name}_{self.job_start_time}.log"
        
    def log(self, level, message):
        """Add log entry to buffer"""
        timestamp = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
        log_entry = f"{timestamp} {level}: {message}"
        self.log_buffer.append(log_entry)
        print(log_entry)  # Also print to CloudWatch
        
        # Upload logs periodically to avoid memory issues
        if len(self.log_buffer) >= 50:
            self.upload_logs(append=True)
        
    def info(self, message):
        self.log(level="INFO", message=message)
        
    def error(self, message):
        self.log(level="ERROR", message=message)
        
    def warning(self, message):
        self.log(level="WARNING", message=message)
        
    def upload_logs(self, append=True):
        """Upload buffered logs to S3"""
        if not self.log_buffer:
            return
            
        try:
            existing_content = ""
            if append:
                try:
                    obj = self.s3.get_object(Bucket=self.bucket, Key=self.log_file_key)
                    existing_content = obj['Body'].read().decode('utf-8') + "\n"
                except self.s3.exceptions.NoSuchKey:
                    pass  # File doesn't exist yet
            
            new_content = existing_content + "\n".join(self.log_buffer)
            
            self.s3.put_object(
                Bucket=self.bucket,
                Key=self.log_file_key,
                Body=new_content.encode('utf-8'),
                ContentType='text/plain'
            )
            
            print(f"Uploaded logs to s3://{self.bucket}/{self.log_file_key}")
            self.log_buffer.clear()
            
        except Exception as e:
            print(f"Failed to upload logs to S3: {e}")

class IncrementalDeltaProcessor:
    """Enhanced Delta Lake processor with incremental processing and S3 logging"""

    def __init__(self, spark_session, bucket_name="lab5-cicd", job_name="transform_job"):
        self.spark = spark_session
        self.bucket_name = bucket_name
        self.job_name = job_name
        self.raw_base_path = f"s3://{bucket_name}/raw"
        self.processed_base_path = f"s3://{bucket_name}/processed"
        self.state_path = f"s3://{bucket_name}/state"
        self.s3_client = boto3.client('s3')
        
        # Initialize S3 logging
        self.s3_logger = S3LogHandler(bucket_name, "logs", job_name)
        
        # State tracking for incremental processing
        self.processed_registry_uri = f"s3://{bucket_name}/state/transform_processed_files.json"
        
        # Configure Delta Lake optimizations
        self._configure_delta_optimizations()
        
        # Setup standard logging
        logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s')
        self.logger = logging.getLogger(__name__)

        # Table configurations
        self.table_configs = {
            "orders": {
                "primary_key": "order_id",
                "partition_columns": ["date"],
                "schema": self._get_orders_schema(),
                "transformations": self._transform_orders
            },
            "order_items": {
                "primary_key": "id", 
                "partition_columns": ["date"],
                "schema": self._get_order_items_schema(),
                "transformations": self._transform_order_items
            },
            "products": {
                "primary_key": "product_id",
                "partition_columns": ["department"],
                "schema": self._get_products_schema(),
                "transformations": self._transform_products
            }
        }

    def _configure_delta_optimizations(self):
        """Configure Spark for optimal Delta Lake performance"""
        configs = {
            # "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
            # "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            "spark.databricks.delta.optimizeWrite.enabled": "true",
            "spark.databricks.delta.autoCompact.enabled": "true",
            "spark.databricks.delta.retentionDurationCheck.enabled": "false",
            "spark.sql.adaptive.enabled": "true",
            "spark.sql.adaptive.coalescePartitions.enabled": "true"
            # "spark.serializer": "org.apache.spark.serializer.KryoSerializer"
        }

        for key, value in configs.items():
            self.spark.conf.set(key, value)

    def _get_orders_schema(self):
        """Define schema for orders table"""
        return StructType([
            StructField("order_num", IntegerType(), True),
            StructField("order_id", LongType(), False),
            StructField("user_id", LongType(), False),
            StructField("order_timestamp", TimestampType(), True),
            StructField("total_amount", DecimalType(10, 2), True),
            StructField("date", DateType(), False)
        ])

    def _get_order_items_schema(self):
        """Define schema for order_items table"""
        return StructType([
            StructField("id", LongType(), False),
            StructField("order_id", LongType(), False),
            StructField("user_id", LongType(), False),
            StructField("days_since_prior_order", IntegerType(), True),
            StructField("product_id", LongType(), False),
            StructField("add_to_cart_order", IntegerType(), True),
            StructField("reordered", IntegerType(), True),
            StructField("order_timestamp", TimestampType(), True),
            StructField("date", DateType(), False)
        ])

    def _get_products_schema(self):
        """Define schema for products table"""
        return StructType([
            StructField("product_id", LongType(), False),
            StructField("department_id", IntegerType(), True),
            StructField("department", StringType(), True),
            StructField("name", StringType(), True)
        ])

    def _transform_orders(self, df):
        """Apply transformations specific to orders table"""
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
        """Apply transformations specific to order_items table"""
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
        """Apply transformations specific to products table"""
        return (df
                .withColumn("product_id", F.col("product_id").cast(LongType()))
                .withColumn("department_id", F.col("department_id").cast(IntegerType()))
                .withColumnRenamed("product_name", "name")
                .withColumn("name_length", F.length("name"))
                .withColumn("created_date", F.current_date()))

    def _get_file_modification_time(self, bucket, key):
        """Get file modification time from S3"""
        try:
            response = self.s3_client.head_object(Bucket=bucket, Key=key)
            return response['LastModified']
        except Exception as e:
            self.s3_logger.error(f"Failed to get modification time for {key}: {e}")
            return None

    def _load_processed_registry(self):
        """Load processed files registry"""
        try:
            obj = self.s3_client.get_object(Bucket=self.bucket_name, Key=self.processed_registry_uri.split('/', 3)[-1])
            return json.loads(obj['Body'].read().decode('utf-8'))
        except self.s3_client.exceptions.NoSuchKey:
            return {}
        except Exception as e:
            self.s3_logger.error(f"Error loading processed registry: {e}")
            return {}

    def _save_processed_registry(self, registry):
        """Save processed files registry"""
        try:
            key = self.processed_registry_uri.split('/', 3)[-1]
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=key,
                Body=json.dumps(registry, indent=2).encode('utf-8'),
                ContentType='application/json'
            )
        except Exception as e:
            self.s3_logger.error(f"Error saving processed registry: {e}")

    def _should_process_file(self, table_name, processed_registry):
        """Check if file should be processed based on modification time"""
        raw_key = f"raw/{table_name}/combined.csv"
        
        try:
            # Check if file exists
            self.s3_client.head_object(Bucket=self.bucket_name, Key=raw_key)
        except self.s3_client.exceptions.NoSuchKey:
            self.s3_logger.info(f"No raw data file found for {table_name}")
            return False
        
        current_mod_time = self._get_file_modification_time(self.bucket_name, raw_key)
        if not current_mod_time:
            return False
            
        # Check if we've processed this file before
        if table_name in processed_registry:
            last_processed_time = datetime.fromisoformat(processed_registry[table_name]['last_processed'])
            if current_mod_time.replace(tzinfo=None) <= last_processed_time:
                self.s3_logger.info(f"File {table_name}/combined.csv hasn't been modified since last processing")
                return False
        
        return True

    def _check_data_quality(self, df, table_name, config):
        """Perform data quality checks"""
        quality_checks = {
            "total_records": df.count(),
            "null_primary_key": df.filter(F.col(config["primary_key"]).isNull()).count(),
            "duplicate_primary_key": df.count() - df.dropDuplicates([config["primary_key"]]).count(),
            "null_date_records": df.filter(F.col("date").isNull()).count() if "date" in df.columns else 0
        }

        # Log quality metrics
        self.s3_logger.info(f"Data Quality Metrics for {table_name}:")
        for metric, value in quality_checks.items():
            self.s3_logger.info(f"  {metric}: {value}")

        # Validate critical quality rules
        if quality_checks["null_primary_key"] > 0:
            raise ValueError(f"Found {quality_checks['null_primary_key']} records with null primary key in {table_name}")
        
        if quality_checks["total_records"] == 0:
            self.s3_logger.warning(f"No records found for table {table_name}")

        return quality_checks

    def _write_quality_metrics(self, table_name, metrics):
        """Write quality metrics to S3"""
        metrics_with_timestamp = {
            "table_name": table_name,
            "timestamp": datetime.utcnow().isoformat(),
            "metrics": metrics
        }

        metrics_key = f"state/quality_metrics/{table_name}_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.json"
        try:
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=metrics_key,
                Body=json.dumps(metrics_with_timestamp, indent=2),
                ContentType='application/json'
            )
            self.s3_logger.info(f"Quality metrics written to s3://{self.bucket_name}/{metrics_key}")
        except Exception as e:
            self.s3_logger.error(f"Failed to write quality metrics: {e}")

    def process_table(self, table_name, mode="merge"):
        """Process a single table with incremental processing"""
        try:
            self.s3_logger.info(f"Starting processing for table: {table_name}")
            
            if table_name not in self.table_configs:
                raise ValueError(f"Unknown table: {table_name}")

            config = self.table_configs[table_name]
            raw_path = f"{self.raw_base_path}/{table_name}/combined.csv"
            processed_path = f"{self.processed_base_path}/{table_name}"

            # Check if raw data exists and needs processing
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

            # Remove duplicates based on primary key
            cleaned_df = transformed_df.dropDuplicates([config["primary_key"]])

            # Data quality checks
            quality_metrics = self._check_data_quality(cleaned_df, table_name, config)
            self._write_quality_metrics(table_name, quality_metrics)

            # Write data based on mode
            if mode == "overwrite":
                self._write_overwrite(cleaned_df, processed_path, config)
            elif mode == "append":
                self._write_append(cleaned_df, processed_path, config)
            elif mode == "merge":
                self._write_merge(cleaned_df, processed_path, config, table_name)
            else:
                raise ValueError(f"Unsupported write mode: {mode}")

            # Optimize table
            self._optimize_table(processed_path)

            self.s3_logger.info(f"Successfully processed {quality_metrics['total_records']} records for {table_name}")
            return True

        except Exception as e:
            self.s3_logger.error(f"Error processing table {table_name}: {e}")
            raise

    def _write_overwrite(self, df, path, config):
        """Write data in overwrite mode"""
        (df.write
         .format("delta")
         .mode("overwrite")
         .partitionBy(*config["partition_columns"])
         .option("overwriteSchema", "true")
         .save(path))

    def _write_append(self, df, path, config):
        """Write data in append mode"""
        (df.write
         .format("delta")
         .mode("append")
         .partitionBy(*config["partition_columns"])
         .save(path))

    def _write_merge(self, df, path, config, table_name):
        """Write data using Delta merge (upsert)"""
        primary_key = config["primary_key"]

        # Create table if it doesn't exist
        if not self._table_exists(path):
            self._write_overwrite(df, path, config)
            return

        # Perform merge operation
        delta_table = DeltaTable.forPath(self.spark, path)
        (delta_table.alias("target")
         .merge(df.alias("source"), f"target.{primary_key} = source.{primary_key}")
         .whenMatchedUpdateAll()
         .whenNotMatchedInsertAll()
         .execute())

        self.s3_logger.info(f"Completed merge operation for {table_name}")

    def _table_exists(self, path):
        """Check if Delta table exists"""
        try:
            DeltaTable.forPath(self.spark, path)
            return True
        except:
            return False

    def _optimize_table(self, path):
        """Optimize Delta table"""
        try:
            delta_table = DeltaTable.forPath(self.spark, path)
            delta_table.optimize().executeCompaction()
            self.s3_logger.info(f"Optimized Delta table at {path}")
        except Exception as e:
            self.s3_logger.warning(f"Failed to optimize table at {path}: {e}")

    def process_all_tables(self, mode="merge"):
        """Process all tables with incremental processing and error isolation"""
        self.s3_logger.info("Starting incremental Delta Lake processing job")
        
        # Load processed files registry
        processed_registry = self._load_processed_registry()
        
        results = {}
        processed_count = 0
        skipped_count = 0
        
        for table_name in self.table_configs.keys():
            try:
                # Check if file needs processing
                if not self._should_process_file(table_name, processed_registry):
                    self.s3_logger.info(f"Skipping {table_name} - no new data to process")
                    results[table_name] = "skipped"
                    skipped_count += 1
                    continue
                
                # Process the table
                success = self.process_table(table_name, mode)
                results[table_name] = success
                
                if success:
                    processed_count += 1
                    # Update processed registry
                    processed_registry[table_name] = {
                        "last_processed": datetime.utcnow().isoformat(),
                        "status": "completed"
                    }
                    
            except Exception as e:
                self.s3_logger.error(f"Failed to process {table_name}: {e}")
                results[table_name] = False

        # Save updated registry
        self._save_processed_registry(processed_registry)

        # Summary logging
        total = len(self.table_configs)
        failed_count = sum(1 for result in results.values() if result is False)
        
        self.s3_logger.info(f"Processing complete: {processed_count} processed, {skipped_count} skipped, {failed_count} failed out of {total} total tables")
        
        if failed_count > 0:
            failed_tables = [table for table, result in results.items() if result is False]
            self.s3_logger.error(f"Failed tables: {failed_tables}")

        # Final log upload
        self.s3_logger.upload_logs(append=True)
        
        return results

def main():
    """Main execution function"""
    # Initialize Spark and Glue contexts
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session

    # Get job parameters
    args = getResolvedOptions(sys.argv, ['JOB_NAME'])
    job = Job(glueContext)
    job.init(args['JOB_NAME'], args)

    try:
        # Initialize processor
        processor = IncrementalDeltaProcessor(spark, job_name=args['JOB_NAME'])

        # Process all tables with incremental logic
        results = processor.process_all_tables(mode="merge")

        # Check results
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





{
  "Comment": "Lakehouse ETL Workflow triggered by S3 event via Lambda with Archiving.",
  "StartAt": "StartExtractJob",
  "States": {
    "StartExtractJob": {
      "Type": "Task",
      "Resource": "arn:aws:states:::glue:startJobRun.sync",
      "Parameters": {
        "JobName": "Extract_data",
        "Arguments": {
          "--JOB_NAME": "Extract_data",
          "--S3_BUCKET": "$.s3Event.bucket",
          "--S3_KEY": "$.s3Event.key"
        }
      },
      "Next": "StartTransformJob",
      "Catch": [
        {
          "ErrorEquals": ["States.ALL"],
          "Next": "HandleFailure"
        }
      ],
      "TimeoutSeconds": 3600
    },
    "StartTransformJob": {
      "Type": "Task",
      "Resource": "arn:aws:states:::glue:startJobRun.sync",
      "Parameters": {
        "JobName": "Transform_data",
        "Arguments": {
          "--JOB_NAME": "Transform_data",
          "--S3_BUCKET": "$.s3Event.bucket",
          "--S3_KEY": "$.s3Event.key"
        }
      },
      "Next": "ArchiveSourceFile",
      "Catch": [
        {
          "ErrorEquals": ["States.ALL"],
          "Next": "HandleFailure"
        }
      ],
      "TimeoutSeconds": 3600
    },
    "ArchiveSourceFile": {
      "Comment": "Archives the original file to the /archived/ prefix.",
      "Type": "Task",
      "Resource": "arn:aws:states:::aws-sdk:s3:copyObject",
      "Parameters": {
        "Bucket": "$.s3Event.bucket",
        "CopySource": {
          "Bucket": "$.s3Event.bucket",
          "Key": "$.s3Event.key"
        },
        "Key": "archived/$.s3Event.key"
      },
      "Next": "DeleteOriginalFile",
      "Catch": [
        {
          "ErrorEquals": ["States.ALL"],
          "Next": "HandleFailure"
        }
      ]
    },
    "DeleteOriginalFile": {
      "Comment": "Deletes the original file from the source bucket.",
      "Type": "Task",
      "Resource": "arn:aws:states:::aws-sdk:s3:deleteObject",
      "Parameters": {
        "Bucket": "$.s3Event.bucket",
        "Key": "$.s3Event.key"
      },
      "Next": "RunGlueCrawler",
      "Catch": [
        {
          "ErrorEquals": ["States.ALL"],
          "Next": "HandleFailure"
        }
      ]
    },
    "RunGlueCrawler": {
      "Type": "Task",
      "Resource": "arn:aws:states:::glue:startCrawler.sync",
      "Parameters": {
        "Name": "lakehouse_crawler"
      },
      "Next": "ValidateDataPresence",
      "Catch": [
        {
          "ErrorEquals": ["States.ALL"],
          "Next": "HandleFailure"
        }
      ],
      "TimeoutSeconds": 1800
    },
    "ValidateDataPresence": {
      "Type": "Task",
      "Resource": "arn:aws:states:::athena:startQueryExecution.sync",
      "Parameters": {
        "QueryExecutionContext": {
          "Database": "lakehouse_dwh"
        },
        "QueryString": "SELECT COUNT(*) as record_count FROM lakehouse_dwh.orders WHERE date = current_date",
        "ResultConfiguration": {
          "OutputLocation": "s3://lab5-cicd/athena-results/"
        }
      },
      "ResultPath": "$.athenaResult",
      "Next": "CheckQueryResults",
      "Catch": [
        {
          "ErrorEquals": ["States.ALL"],
          "Next": "HandleFailure"
        }
      ],
      "TimeoutSeconds": 600
    },
    "CheckQueryResults": {
      "Type": "Task",
      "Resource": "arn:aws:states:::aws-sdk:athena:getQueryResults",
      "Parameters": {
        "QueryExecutionId.$": "$.athenaResult.QueryExecutionId"
      },
      "ResultPath": "$.queryResults",
      "Next": "EvaluateResults",
      "Catch": [
        {
          "ErrorEquals": ["States.ALL"],
          "Next": "HandleFailure"
        }
      ]
    },
    "EvaluateResults": {
      "Type": "Choice",
      "Choices": [
        {
          "Variable": "$.queryResults.ResultSet.Rows[1].Data[0].VarCharValue",
          "NumericGreaterThan": 0,
          "Next": "NotifySuccess"
        }
      ],
      "Default": "HandleFailure"
    },
    "NotifySuccess": {
      "Type": "Task",
      "Resource": "arn:aws:states:::sns:publish",
      "Parameters": {
        "TopicArn": "arn:aws:sns:eu-north-1:476114131873:Lab_5",
        "Subject": "Lakehouse ETL Pipeline - Success",
        "Message": {
          "Status": "SUCCESS",
          "Message": "Lakehouse ETL pipeline completed successfully",
          "ProcessedFile": {
            "Bucket.$": "$.s3Event.bucket",
            "Key.$": "$.s3Event.key"
          },
          "RecordCount.$": "$.queryResults.ResultSet.Rows[1].Data[0].VarCharValue",
          "CompletedAt.$": "$$.State.EnteredTime"
        }
      },
      "Next": "Success",
      "Catch": [
        {
          "ErrorEquals": ["States.ALL"],
          "Next": "Success",
          "Comment": "Continue to success even if SNS notification fails"
        }
      ]
    },
    "HandleFailure": {
      "Type": "Task",
      "Resource": "arn:aws:states:::sns:publish",
      "Parameters": {
        "TopicArn": "arn:aws:sns:eu-north-1:476114131873:Lab_5",
        "Subject": "Lakehouse ETL Pipeline - Failure",
        "Message": {
          "Status": "FAILED",
          "Message": "Lakehouse ETL pipeline failed",
          "ProcessedFile": {
            "Bucket.$": "$.s3Event.bucket",
            "Key.$": "$.s3Event.key"
          },
          "Error.$": "$.Error",
          "Cause.$": "$.Cause",
          "FailedAt.$": "$$.State.EnteredTime"
        }
      },
      "Next": "Fail",
      "Catch": [
        {
          "ErrorEquals": ["States.ALL"],
          "Next": "Fail"
        }
      ]
    },
    "Success": {
      "Type": "Succeed"
    },
    "Fail": {
      "Type": "Fail"
    }
  }
}