import boto3
import logging
from datetime import datetime
import json
from utility import process_monthly_excel_file_with_sheet_tracking, s3_read_json, s3_write_json
class S3LogHandler:
    def __init__(self, bucket, log_key_prefix):
        self.bucket = bucket
        self.log_key_prefix = log_key_prefix
        self.s3 = boto3.client('s3')
        self.log_buffer = []
        self.job_start_time = datetime.utcnow().strftime('%Y-%m-%d_%H-%M-%S')
    def log(self, level, message):
        """Add log entry to buffer"""
        timestamp = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
        log_entry = f"{timestamp} {level}: {message}"
        self.log_buffer.append(log_entry)
        print(log_entry)  # Also print to CloudWatch
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
        log_key = f"{self.log_key_prefix}/glue_job_{self.job_start_time}.log"
        try:
            existing_content = ""
            if append:
                try:
                    obj = self.s3.get_object(Bucket=self.bucket, Key=log_key)
                    existing_content = obj['Body'].read().decode('utf-8') + "\n"
                except self.s3.exceptions.NoSuchKey:
                    pass  # File doesn't exist yet
            new_content = existing_content + "\n".join(self.log_buffer)
            self.s3.put_object(
                Bucket=self.bucket,
                Key=log_key,
                Body=new_content.encode('utf-8'),
                ContentType='text/plain'
            )
            print(f"Uploaded logs to s3://{self.bucket}/{log_key}")
            self.log_buffer.clear()
        except Exception as e:
            print(f"Failed to upload logs to S3: {e}")
# Setup custom logging
s3_logger = S3LogHandler(bucket='lab5-cicd', log_key_prefix='logs')
# Setup standard logging for CloudWatch
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s')
logger = logging.getLogger(__name__)
# S3 clients and buckets
s3 = boto3.client('s3')
SOURCE_BUCKET = 'lab5-source-data'
DEST_BUCKET = 'lab5-cicd'
RAW_PREFIX = 'raw'
STATE_PREFIX = 'state'
PROCESSED_REGISTRY_S3_URI = f's3://{DEST_BUCKET}/{STATE_PREFIX}/processed_files.json'
PROCESSED_IDS_S3_PREFIX = f's3://{DEST_BUCKET}/{STATE_PREFIX}/processed_ids'
def list_source_files():
    """List all files in the source S3 bucket."""
    paginator = s3.get_paginator('list_objects_v2')
    page_iterator = paginator.paginate(Bucket=SOURCE_BUCKET)
    files = []
    for page in page_iterator:
        if 'Contents' in page:
            for obj in page['Contents']:
                files.append(obj['Key'])
    return files
def process_csv_file_in_glue(csv_bytes, filename):
    """Process CSV file using utility function."""
    try:
        from utility import process_csv_file
        s3_logger.info(f"Processing CSV file: {filename}")
        success = process_csv_file(
            csv_bytes=csv_bytes,
            processed_registry_s3_uri=PROCESSED_REGISTRY_S3_URI,
            processed_ids_s3_prefix=PROCESSED_IDS_S3_PREFIX,
            output_bucket=DEST_BUCKET,
            output_prefix=RAW_PREFIX,
            filename=filename,
            chunk_size=10000
        )
        if success:
            s3_logger.info(f"Successfully processed CSV file: {filename}")
        else:
            s3_logger.error(f"Failed to process CSV file: {filename}")
        return success
    except ImportError as e:
        s3_logger.error(f"Could not import process_csv_file function: {e}")
        return False
    except Exception as e:
        s3_logger.error(f"Error processing CSV file {filename}: {e}")
        return False
def process_excel_file_in_glue(bucket, key):
    """Process Excel file using utility function."""
    try:
        s3_logger.info(f"Processing Excel file: {key}")
        success = process_monthly_excel_file_with_sheet_tracking(
            excel_bucket=bucket,
            excel_key=key,
            size_threshold_mb=10,
            chunk_size=5000
        )
        if success:
            s3_logger.info(f"Successfully processed Excel file: {key}")
        else:
            s3_logger.error(f"Failed to process Excel file: {key}")
        return success
    except Exception as e:
        s3_logger.error(f"Error processing Excel file {key}: {e}")
        return False
def is_file_completely_processed(filename, processed_registry):
    """Check if a file has been completely processed."""
    if filename not in processed_registry:
        return False
    if filename.lower().endswith('.csv'):
        return True
    processed_sheets = processed_registry.get(filename, [])
    return len(processed_sheets) > 0
def main():
    """Main processing function."""
    try:
        s3_logger.info("Starting Extract_data Glue job")
        # List all files in source bucket
        files = list_source_files()
        if not files:
            s3_logger.info(f"No files found in source bucket {SOURCE_BUCKET}")
            return
        s3_logger.info(f"Found {len(files)} files in source bucket")
        # Get processed files registry
        processed_registry = s3_read_json(PROCESSED_REGISTRY_S3_URI)
        s3_logger.info(f"Loaded processed registry with {len(processed_registry)} entries")
        processed_count = 0
        skipped_count = 0
        failed_count = 0
        for key in files:
            filename = key.split('/')[-1]
            if '.' not in filename:
                s3_logger.warning(f"File {filename} has no extension, skipping.")
                skipped_count += 1
                continue
            ext = filename.lower().rsplit('.', 1)[-1]
            if is_file_completely_processed(filename, processed_registry):
                s3_logger.info(f"File {filename} already processed. Skipping.")
                skipped_count += 1
                continue
            # Download file from S3
            try:
                s3_logger.info(f"Downloading file: {key}")
                obj = s3.get_object(Bucket=SOURCE_BUCKET, Key=key)
                file_bytes = obj['Body'].read()
                s3_logger.info(f"Downloaded {len(file_bytes)} bytes for {filename}")
            except Exception as e:
                s3_logger.error(f"Failed to download {key} from S3: {e}")
                failed_count += 1
                continue
            # Process based on file type
            success = False
            if ext == 'xlsx':
                success = process_excel_file_in_glue(SOURCE_BUCKET, key)
            elif ext == 'csv':
                success = process_csv_file_in_glue(file_bytes, filename)
            else:
                s3_logger.warning(f"Unsupported file type '{ext}' for {filename}, skipping.")
                skipped_count += 1
                continue
            if success:
                processed_count += 1
            else:
                failed_count += 1
            # Upload logs periodically
            s3_logger.upload_logs(append=True)
        # Log final summary
        s3_logger.info(f"Processing complete. Processed: {processed_count}, Skipped: {skipped_count}, Failed: {failed_count}")
        # Final log upload
        s3_logger.upload_logs(append=True)
    except Exception as e:
        s3_logger.error(f"Critical error in main processing: {e}")
        s3_logger.upload_logs(append=True)
        raise
if __name__ == '__main__':
    main()