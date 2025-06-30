import boto3
from botocore.exceptions import ClientError
import logging

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

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
