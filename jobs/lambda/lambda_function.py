import json
import boto3
import os
from datetime import datetime

def lambda_handler(event, context):
    # Required environment variable
    state_machine_arn = os.environ['STATE_MACHINE_ARN']
    
    # Optional environment variables
    file_prefix_filter = os.environ.get('FILE_PREFIX_FILTER', '')
    file_suffix_filter = os.environ.get('FILE_SUFFIX_FILTER', '')
    execution_name_prefix = os.environ.get('EXECUTION_NAME_PREFIX', 's3-trigger')
    
    # Create Step Functions client
    stepfunctions = boto3.client('stepfunctions')

    try:
        # Process S3 event records
        for record in event.get('Records', []):
            if record.get('eventSource') == 'aws:s3':
                bucket_name = record['s3']['bucket']['name']
                object_key = record['s3']['object']['key']
                event_name = record['eventName']

                # Only process events from 'lab5-source-data' bucket
                if bucket_name != 'lab5-source-data':
                    print(f"Skipping bucket {bucket_name} - not target bucket.")
                    continue

                # Apply optional prefix/suffix filters
                if file_prefix_filter and not object_key.startswith(file_prefix_filter):
                    print(f"Skipping {object_key} - does not match prefix '{file_prefix_filter}'")
                    continue
                if file_suffix_filter and not object_key.endswith(file_suffix_filter):
                    print(f"Skipping {object_key} - does not match suffix '{file_suffix_filter}'")
                    continue

                print(f"Processing S3 object: {bucket_name}/{object_key}")

                # Build a safe execution name
                safe_key = object_key.replace('/', '_').replace('.', '_').replace('-', '_')
                execution_name = f"{execution_name_prefix}_{safe_key}_{int(datetime.now().timestamp())}"
                if len(execution_name) > 80:
                    execution_name = execution_name[:80]

                # Build input for Step Function
                step_function_input = {
                    's3Event': {
                        'bucket': bucket_name,
                        'key': object_key,
                        'eventName': event_name,
                        'eventTime': record['eventTime'],
                        'region': record['awsRegion']
                    },
                    'originalEvent': record
                }

                # Start execution
                response = stepfunctions.start_execution(
                    stateMachineArn=state_machine_arn,
                    name=execution_name,
                    input=json.dumps(step_function_input)
                )

                print(f"Started Step Function execution: {response['executionArn']}")

        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Step Function executions started successfully',
                'processedRecords': len(event.get('Records', []))
            })
        }

    except Exception as e:
        print(f"Error processing S3 event: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': 'Failed to process S3 event',
                'details': str(e)
            })
        }
