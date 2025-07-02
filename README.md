# Lakehouse Architecture Project

## Overview

This project implements a cloud-native Lakehouse Architecture on AWS for processing e-commerce datasets (Products, Orders, and Order Items). It uses AWS Glue with PySpark and Delta Lake for ETL processing, AWS Step Functions for orchestration, AWS Lambda for S3 event triggering, and GitHub Actions for CI/CD automation. The pipeline ingests CSV files from an S3 source bucket, performs data cleansing and transformation, writes to Delta tables, updates the AWS Glue Data Catalog for Athena querying, and archives processed files.

## Features

- **Data Ingestion**: Detects new CSV files in an S3 source bucket using an AWS Lambda function triggered by S3 events.
- **ETL Processing**: Uses AWS Glue jobs to process, validate, and deduplicate data.
- **Delta Lake**: Stores processed data in Delta tables with partitioning and merge/upsert operations.
- **Data Quality**: Enforces schemas, checks for null primary keys, duplicates, and referential integrity, and logs rejected records.
- **Orchestration**: Coordinates the pipeline using AWS Step Functions, triggered by S3 events.
- **Archiving**: Moves processed files to an S3 archived prefix after successful processing.
- **CI/CD**: Automates deployment and testing via GitHub Actions.

## Architecture

1. **S3 Event Trigger**: An AWS Lambda function detects file uploads in the source S3 bucket and triggers a Step Functions state machine.
2. **Extract Stage**: A Glue job lists and processes CSV files, writing to a raw S3 prefix and archiving originals.
3. **Transform Stage**: A Glue job reads raw CSVs, applies transformations, enforces schemas, performs data quality checks, and writes to Delta tables.
4. **Data Catalog**: Updates the AWS Glue Data Catalog for Athena querying (TBD: via Spark SQL or Glue Crawler).
5. **Logging**: Logs activities to an S3 logs prefix and CloudWatch, with rejected records in an S3 rejected records prefix.
6. **CI/CD**: GitHub Actions deploys scripts to an S3 jobs prefix and packages the Lambda function.

## Project Structure

```
├── jobs/
│   ├── etl/
│   │   ├── extract.py          # Glue job for file ingestion and archiving
│   │   ├── transform.py        # Glue job for data transformation and Delta Lake
│   │   ├── utility.py          # Helper functions for CSV processing
│   ├── lambda/
│   │   ├── lambda_function.py  # Lambda function for S3 event triggering
├── tests/
│   ├── unit/                   # Unit tests (TBD)
│   ├── integration/            # Integration tests (TBD)
├── deploy-pipeline.yml         # GitHub Actions CI/CD pipeline
├── requirements.txt            # Python dependencies
├── lab5_stepfunction.json      # Step Functions definition (TBD)
├── README.md                   # Project overview (this file)
├── SETUP.md                    # Setup instructions
```

## Prerequisites

- AWS Account with permissions for S3, Glue, Lambda, Step Functions, and IAM.
- Python 3.9+ for local development and testing.
- AWS CLI configured with credentials.
- GitHub repository with Actions enabled.

## Setup

See SETUP.md for detailed instructions on setting up the AWS resources, environment, and deployment.

## Usage

1. **Upload Data**: Place CSV files for Products, Orders, and Order Items in the source S3 bucket's raw prefix.
2. **Run Pipeline**: The Lambda function automatically triggers on S3 events, starting the Step Functions workflow, which executes the extract and transform Glue jobs.
3. **Query Data**: Use Athena to query Delta tables in the processed S3 prefix after Glue Data Catalog updates.
4. **Monitor Logs**: Check the S3 logs prefix and CloudWatch for processing logs and the S3 rejected records prefix for rejected records.

## Datasets

- **Products**: `product_id` (PK), `department_id`, `department`, `product_name`.
- **Orders**: `order_num`, `order_id` (PK), `user_id`, `order_timestamp`, `total_amount`, `date`.
- **Order Items**: `id` (PK), `order_id`, `user_id`, `days_since_prior_order`, `product_id`, `add_to_cart_order`, `reordered`, `order_timestamp`, `date`.

## Known Issues

- **Bucket Configuration**: The extract and Lambda scripts may use a separate source bucket, while the transform script expects a raw prefix in the main bucket. Align to a single bucket configuration.
- **Excel Processing**: The utility script processes Excel files, which may not be required.
- **Glue Data Catalog**: Catalog updates are not yet implemented in provided scripts.
- **Testing**: CI/CD testing jobs are commented out; tests need to be implemented.

## Next Steps

- Share `lab5_stepfunction.json` to verify Step Functions orchestration.
- Implement Glue Data Catalog updates via Spark SQL or a Glue Crawler.
- Remove Excel processing if not required.
- Enable CI/CD testing jobs and implement unit/integration tests.
- Align S3 bucket configuration across all scripts.
- Move archiving to post-transformation in Step Functions or the transform script.

## 