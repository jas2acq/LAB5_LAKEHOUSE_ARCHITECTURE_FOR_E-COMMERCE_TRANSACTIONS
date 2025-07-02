import boto3
from botocore.exceptions import ClientError
import logging
import os
import io
import json
import pandas as pd
from openpyxl.utils.exceptions import InvalidFileException

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

# Global constants
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


def create_s3_bucket(bucket_name, region='eu-north-1'):
    """
    Creates an S3 bucket in a specified region.

    Args:
        bucket_name (str): The name of the S3 bucket to create.
        region (str, optional): The AWS region to create the bucket in.
                                Defaults to 'eu-north-1'.

    Returns:
        bool: True if the bucket was created successfully or already exists, False otherwise.
    """
    try:
        s3_client = boto3.client('s3', region_name=region)

        # Check if bucket exists
        try:
            s3_client.head_bucket(Bucket=bucket_name)
            logging.info(f"Bucket '{bucket_name}' already exists.")
            return True
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == '404':
                logging.info(f"Bucket '{bucket_name}' does not exist. Creating...")
                if region == 'us-east-1':
                    s3_client.create_bucket(Bucket=bucket_name)
                else:
                    s3_client.create_bucket(
                        Bucket=bucket_name,
                        CreateBucketConfiguration={'LocationConstraint': region}
                    )
                logging.info(f"Bucket '{bucket_name}' created successfully in region '{region}'.")
                return True
            else:
                logging.error(f"Error checking bucket '{bucket_name}' existence: {e}")
                return False

    except ClientError as e:
        logging.error(f"Failed to create S3 bucket '{bucket_name}': {e}")
        return False
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")
        return False


def s3_read_json(s3_uri):
    """Read JSON file from S3."""
    s3 = boto3.client('s3')
    bucket, key = s3_uri.replace("s3://", "").split("/", 1)
    try:
        obj = s3.get_object(Bucket=bucket, Key=key)
        return json.loads(obj['Body'].read().decode('utf-8'))
    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchKey':
            return {}
        logger.error(f"Error reading JSON from {s3_uri}: {e}")
        return {}
    except Exception as e:
        logger.error(f"Unexpected error reading JSON: {e}")
        return {}


def s3_write_json(s3_uri, data):
    """Write JSON data to S3."""
    s3 = boto3.client('s3')
    bucket, key = s3_uri.replace("s3://", "").split("/", 1)
    try:
        s3.put_object(Bucket=bucket, Key=key, Body=json.dumps(data, indent=2).encode('utf-8'))
    except Exception as e:
        logger.error(f"Error writing JSON to {s3_uri}: {e}")


def s3_read_csv_to_df(bucket, key):
    """Read CSV from S3 to DataFrame."""
    s3 = boto3.client('s3')
    try:
        obj = s3.get_object(Bucket=bucket, Key=key)
        return pd.read_csv(io.BytesIO(obj['Body'].read()))
    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchKey':
            return pd.DataFrame()
        else:
            logger.error(f"Error reading CSV from s3://{bucket}/{key}: {e}")
            raise
    except Exception as e:
        logger.error(f"Unexpected error reading CSV: {e}")
        raise


def s3_write_df_to_csv(df, bucket, key):
    """Write DataFrame to S3 as CSV."""
    s3 = boto3.client('s3')
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)
    s3.put_object(Bucket=bucket, Key=key, Body=csv_buffer.getvalue().encode('utf-8'))
    logger.info(f"Uploaded combined CSV to s3://{bucket}/{key}")


def identify_data_type(df):
    """Identify the data type based on DataFrame columns."""
    for data_type, cols in EXPECTED_COLUMNS.items():
        if set(df.columns) == cols:
            return data_type
    return None


def process_csv_file(
    csv_bytes,
    processed_registry_s3_uri,
    processed_ids_s3_prefix,
    output_bucket,
    output_prefix,
    filename=None,
    size_threshold_mb=10,
    chunk_size=10000
):
    """
    Processes CSV files with size-based chunking and deduplication.
    If file size < size_threshold_mb, loads entire CSV at once.
    Otherwise, processes CSV in chunks.

    Args:
        csv_bytes (bytes): CSV file content as bytes.
        processed_registry_s3_uri (str): S3 URI for processed files registry JSON.
        processed_ids_s3_prefix (str): S3 prefix for processed IDs JSON files.
        output_bucket (str): S3 bucket for output CSV files.
        output_prefix (str): S3 prefix for output CSV files.
        filename (str): Actual filename for registry tracking (optional).
        size_threshold_mb (int): Size threshold in MB to decide chunking.
        chunk_size (int): Number of rows per chunk when chunking.

    Returns:
        bool: True if processing succeeded, False otherwise.
    """
    # Import the functions from the same module (they should be in the same file)
    # Remove the 'from .' import as it won't work in this context
    
    # Use actual filename if provided, otherwise use placeholder
    registry_filename = filename if filename else "in_memory_csv.csv"
    file_size_mb = len(csv_bytes) / (1024 * 1024)
    
    logger.info(f"Processing CSV file: {registry_filename} ({file_size_mb:.2f} MB)")

    try:
        processed_registry = s3_read_json(processed_registry_s3_uri) or {}
        processed_ids = {}
        combined_dfs = {dt: pd.DataFrame() for dt in EXPECTED_COLUMNS.keys()}

        if file_size_mb < size_threshold_mb:
            logger.info(f"Using full CSV processing for small file ({file_size_mb:.2f} MB)")
            # Load entire CSV at once
            df = pd.read_csv(io.BytesIO(csv_bytes))
            
            if df.empty:
                logger.info("CSV file is empty. Skipping.")
                processed_registry[registry_filename] = True
                s3_write_json(processed_registry_s3_uri, processed_registry)
                return True
            
            data_type = identify_data_type(df)
            if not data_type:
                logger.warning("CSV columns do not match any expected schema. Skipping file.")
                logger.warning(f"Found columns: {list(df.columns)}")
                logger.warning(f"Expected schemas: {EXPECTED_COLUMNS}")
                return False

            # Load processed IDs for this data type
            if data_type not in processed_ids:
                ids_json = s3_read_json(f"{processed_ids_s3_prefix}/{data_type}_ids.json")
                processed_ids[data_type] = set(ids_json.get('ids', [])) if ids_json else set()

            # Filter out already processed records
            key_col = UNIQUE_KEYS[data_type]
            if key_col not in df.columns:
                logger.error(f"Key column '{key_col}' not found in CSV. Available columns: {list(df.columns)}")
                return False
                
            df_filtered = df[~df[key_col].astype(str).isin(processed_ids[data_type])]

            if df_filtered.empty:
                logger.info("All rows in CSV are duplicates. Skipping append.")
            else:
                logger.info(f"Found {len(df_filtered)} new records out of {len(df)} total")
                combined_dfs[data_type] = df_filtered
                processed_ids[data_type].update(df_filtered[key_col].astype(str).tolist())

        else:
            logger.info(f"Using chunked CSV processing for large file ({file_size_mb:.2f} MB)")
            # Process CSV in chunks
            try:
                chunk_iter = pd.read_csv(io.BytesIO(csv_bytes), chunksize=chunk_size)
                chunk_count = 0
                
                for chunk in chunk_iter:
                    chunk_count += 1
                    if chunk.empty:
                        continue
                        
                    data_type = identify_data_type(chunk)
                    if not data_type:
                        logger.warning(f"CSV chunk {chunk_count} columns do not match any expected schema. Skipping chunk.")
                        logger.warning(f"Chunk columns: {list(chunk.columns)}")
                        continue

                    # Load processed IDs for this data type if not already loaded
                    if data_type not in processed_ids:
                        ids_json = s3_read_json(f"{processed_ids_s3_prefix}/{data_type}_ids.json")
                        processed_ids[data_type] = set(ids_json.get('ids', [])) if ids_json else set()

                    # Filter out already processed records
                    key_col = UNIQUE_KEYS[data_type]
                    if key_col not in chunk.columns:
                        logger.error(f"Key column '{key_col}' not found in chunk {chunk_count}")
                        continue
                        
                    chunk_filtered = chunk[~chunk[key_col].astype(str).isin(processed_ids[data_type])]

                    if chunk_filtered.empty:
                        logger.info(f"All rows in CSV chunk {chunk_count} are duplicates for data type '{data_type}', skipping.")
                        continue

                    logger.info(f"Chunk {chunk_count}: Found {len(chunk_filtered)} new records out of {len(chunk)}")
                    combined_dfs[data_type] = pd.concat([combined_dfs[data_type], chunk_filtered], ignore_index=True)
                    processed_ids[data_type].update(chunk_filtered[key_col].astype(str).tolist())
                    
                logger.info(f"Processed {chunk_count} chunks total")
                
            except Exception as e:
                logger.error(f"Error during chunked CSV processing: {e}")
                return False

        # Append combined data per data_type to existing CSV in S3
        total_new_records = 0
        for data_type, new_data in combined_dfs.items():
            if new_data.empty:
                continue

            logger.info(f"Uploading {len(new_data)} new {data_type} records")
            s3_key = f"{output_prefix}/{data_type}/combined.csv"

            try:
                existing_df = s3_read_csv_to_df(output_bucket, s3_key)
                logger.info(f"Found existing {data_type} CSV with {len(existing_df)} records")
            except Exception as e:
                logger.info(f"No existing CSV found for {data_type}, creating new one: {e}")
                existing_df = pd.DataFrame()

            # Combine and upload
            combined_df = pd.concat([existing_df, new_data], ignore_index=True)
            s3_write_df_to_csv(combined_df, output_bucket, s3_key)
            total_new_records += len(new_data)

            # Update processed IDs JSON in S3
            if data_type in processed_ids:
                s3_write_json(f"{processed_ids_s3_prefix}/{data_type}_ids.json", 
                             {'ids': list(processed_ids[data_type])})

        # Mark this file as processed in registry and save
        processed_registry[registry_filename] = True
        s3_write_json(processed_registry_s3_uri, processed_registry)

        logger.info(f"CSV file processed successfully. Total new records: {total_new_records}")
        return True

    except Exception as e:
        logger.error(f"Error processing CSV file: {e}")
        return False



def process_excel_in_chunks(excel_bytes, sheet_name=None, chunk_size=5000):
    """
    Generator to process Excel file in chunks without loading entire file.
    Yields DataFrame chunks from a specific sheet.
    """
    excel_io = io.BytesIO(excel_bytes)
    
    try:
        # Read headers first
        excel_io.seek(0)
        headers_df = pd.read_excel(excel_io, sheet_name=sheet_name, nrows=0)
        headers = headers_df.columns.tolist()
        
        skiprows = 0
        chunk_num = 0
        
        while True:
            excel_io.seek(0)
            try:
                if chunk_num == 0:
                    # First chunk: include headers
                    df_chunk = pd.read_excel(excel_io, sheet_name=sheet_name, nrows=chunk_size)
                    if len(df_chunk) == 0:
                        break
                else:
                    # Subsequent chunks: skip previous rows + header
                    df_chunk = pd.read_excel(
                        excel_io,
                        sheet_name=sheet_name,
                        skiprows=skiprows + 1,  # +1 to skip header row
                        nrows=chunk_size,
                        names=headers
                    )
                    if len(df_chunk) == 0:
                        break
                
                yield df_chunk
                skiprows += len(df_chunk)
                chunk_num += 1
                
            except Exception as e:
                logger.error(f"Error reading Excel chunk {chunk_num}: {e}")
                break
                
    except Exception as e:
        logger.error(f"Error initializing Excel chunked reading: {e}")
        return


def process_monthly_excel_file_with_sheet_tracking(
    excel_bucket,
    excel_key,
    size_threshold_mb=10,
    chunk_size=5000
):
    """
    Cloud-native Excel processor with chunked reading for large files.
    """
    s3 = boto3.client('s3')
    processed_registry_s3_uri = 's3://lab5-cicd/state/processed_files.json'
    processed_ids_s3_prefix = 's3://lab5-cicd/state/processed_ids'
    output_bucket = 'lab5-cicd'
    output_prefix = 'raw'

    try:
        # Read Excel file from S3
        obj = s3.get_object(Bucket=excel_bucket, Key=excel_key)
        excel_bytes = obj['Body'].read()
        file_size_mb = len(excel_bytes) / (1024 * 1024)
        filename = excel_key.split('/')[-1]

        logger.info(f"Processing Excel file: {filename} ({file_size_mb:.2f} MB)")

        # Load processed registry and initialize tracking
        processed_registry = s3_read_json(processed_registry_s3_uri)
        processed_sheets = set(processed_registry.get(filename, []))
        processed_ids = {}
        combined_dfs = {dt: pd.DataFrame() for dt in EXPECTED_COLUMNS.keys()}

        # Read Excel file structure
        excel_io = io.BytesIO(excel_bytes)
        xls = pd.ExcelFile(excel_io, engine='openpyxl')

        for sheet_name in xls.sheet_names:
            if sheet_name in processed_sheets:
                logger.info(f"Sheet '{sheet_name}' already processed. Skipping.")
                continue

            logger.info(f"Processing sheet: {sheet_name}")

            # Decide chunked or full sheet processing
            if file_size_mb > size_threshold_mb:
                logger.info(f"Using chunked processing for large file ({file_size_mb:.2f} MB)")
                # Chunked processing
                for df_chunk in process_excel_in_chunks(excel_bytes, sheet_name, chunk_size):
                    if df_chunk.empty:
                        continue
                        
                    sheet_type = identify_data_type(df_chunk)
                    if not sheet_type:
                        logger.warning(f"Chunk from sheet '{sheet_name}' columns do not match any expected schema. Skipping chunk.")
                        continue

                    # Load processed IDs if not already loaded
                    if sheet_type not in processed_ids:
                        ids_json = s3_read_json(f"{processed_ids_s3_prefix}/{sheet_type}_ids.json")
                        processed_ids[sheet_type] = set(ids_json.get('ids', []))

                    # Filter out already processed records
                    key_col = UNIQUE_KEYS[sheet_type]
                    if key_col in df_chunk.columns:
                        df_chunk = df_chunk[~df_chunk[key_col].astype(str).isin(processed_ids[sheet_type])]
                        
                        if not df_chunk.empty:
                            combined_dfs[sheet_type] = pd.concat([combined_dfs[sheet_type], df_chunk], ignore_index=True)
                            processed_ids[sheet_type].update(df_chunk[key_col].astype(str).tolist())
                    else:
                        logger.error(f"Key column '{key_col}' not found in chunk from sheet '{sheet_name}'")
            else:
                logger.info(f"Using full sheet processing for small file ({file_size_mb:.2f} MB)")
                # Full sheet processing
                excel_io.seek(0)
                df = pd.read_excel(excel_io, sheet_name=sheet_name)
                
                if df.empty:
                    processed_sheets.add(sheet_name)
                    continue

                sheet_type = identify_data_type(df)
                if not sheet_type:
                    logger.warning(f"Sheet '{sheet_name}' columns do not match any expected schema. Skipping.")
                    processed_sheets.add(sheet_name)
                    continue

                # Load processed IDs if not already loaded
                if sheet_type not in processed_ids:
                    ids_json = s3_read_json(f"{processed_ids_s3_prefix}/{sheet_type}_ids.json")
                    processed_ids[sheet_type] = set(ids_json.get('ids', []))

                # Filter out already processed records
                key_col = UNIQUE_KEYS[sheet_type]
                if key_col in df.columns:
                    df = df[~df[key_col].astype(str).isin(processed_ids[sheet_type])]
                    
                    if not df.empty:
                        combined_dfs[sheet_type] = pd.concat([combined_dfs[sheet_type], df], ignore_index=True)
                        processed_ids[sheet_type].update(df[key_col].astype(str).tolist())
                else:
                    logger.error(f"Key column '{key_col}' not found in sheet '{sheet_name}'")

            processed_sheets.add(sheet_name)

        # Append combined data per data_type to existing CSV in S3
        for data_type, new_data in combined_dfs.items():
            if new_data.empty:
                continue
                
            logger.info(f"Uploading {len(new_data)} new {data_type} records")
            s3_key = f"{output_prefix}/{data_type}/combined.csv"
            
            try:
                existing_df = s3_read_csv_to_df(output_bucket, s3_key)
            except Exception as e:
                logger.info(f"No existing CSV found for {data_type}, creating new one")
                existing_df = pd.DataFrame()
            
            combined_df = pd.concat([existing_df, new_data], ignore_index=True)
            s3_write_df_to_csv(combined_df, output_bucket, s3_key)
            
            # Update processed IDs
            if data_type in processed_ids:
                s3_write_json(f"{processed_ids_s3_prefix}/{data_type}_ids.json", 
                             {'ids': list(processed_ids[data_type])})

        # Update processed registry
        processed_registry[filename] = list(processed_sheets)
        s3_write_json(processed_registry_s3_uri, processed_registry)
        
        logger.info(f"Finished processing Excel file {filename}")
        return True
        
    except InvalidFileException:
        logger.error("Invalid Excel file format")
        return False
    except Exception as e:
        logger.error(f"Error processing Excel file: {e}")
        return False