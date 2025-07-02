# Setup Instructions for Lakehouse Architecture Project

This document provides step-by-step instructions to set up the AWS environment, dependencies, and CI/CD pipeline for the Lakehouse Architecture project. The setup assumes familiarity with AWS services and the AWS CLI, using generic placeholders for bucket names and paths to ensure flexibility across environments.

## Prerequisites
- **AWS Account**: Access to S3, Glue, Lambda, Step Functions, IAM, and CloudWatch.
- **AWS CLI**: Installed and configured with credentials (`aws configure`).
- **Python**: Version 3.9+ for local development and testing.
- **Git**: Installed for cloning the repository.
- **GitHub Repository**: With Actions enabled for CI/CD.
- **Docker**: Optional for local testing of Glue jobs (if using AWS Glue local development).

## Step 1: Clone the Repository
```bash
git clone <repository-url>
cd <repository-name>
```

## Step 2: Set Up AWS Resources

### 1. Create S3 Buckets
Create the required S3 buckets in your preferred region (e.g., `eu-north-1`):
```bash
aws s3api create-bucket --bucket <bucket-name> --region <region> --create-bucket-configuration LocationConstraint=<region>
aws s3api create-bucket --bucket <source-bucket-name> --region <region> --create-bucket-configuration LocationConstraint=<region>
```
Create bucket structure for the main bucket:
```bash
aws s3api put-object --bucket <bucket-name> --key raw/
aws s3api put-object --bucket <bucket-name> --key processed/
aws s3api put-object --bucket <bucket-name> --key archived/
aws s3api put-object --bucket <bucket-name> --key logs/
aws s3api put-object --bucket <bucket-name> --key rejected_records/
aws s3api put-object --bucket <bucket-name> --key state/
aws s3api put-object --bucket <bucket-name> --key jobs/etl/
aws s3api put-object --bucket <bucket-name> --key jobs/lambda/
```

### 2. Create IAM Roles
Create IAM roles for Glue and Lambda with the necessary permissions.

#### Glue Execution Role
1. Create a role (e.g., `GlueExecutionRole`) with the following policies:
   - `AWSGlueServiceRole`
   - `AmazonS3FullAccess` (or scoped to `<bucket-name>` and `<source-bucket-name>`)
   - `CloudWatchLogsFullAccess`
   - Custom policy for Delta Lake:
     ```json
     {
         "Version": "2012-10-17",
         "Statement": [
             {
                 "Effect": "Allow",
                 "Action": [
                     "s3:GetObject",
                     "s3:PutObject",
                     "s3:DeleteObject"
                 ],
                 "Resource": [
                     "arn:aws:s3:::<bucket-name>/*",
                     "arn:aws:s3:::<source-bucket-name>/*"
                 ]
             }
         ]
     }
     ```
2. Attach a trust policy for Glue:
   ```json
   {
       "Version": "2012-10-17",
       "Statement": [
           {
               "Effect": "Allow",
               "Principal": {
                   "Service": "glue.amazonaws.com"
               },
               "Action": "sts:AssumeRole"
           }
       ]
   }
   ```

#### Lambda Execution Role
1. Create a role (e.g., `LambdaExecutionRole`) with the following policies:
   - `AWSLambdaBasicExecutionRole`
   - `AWSStepFunctionsFullAccess`
   - `AmazonS3ReadOnlyAccess` (or scoped to `<source-bucket-name>`)
2. Attach a trust policy for Lambda:
   ```json
   {
       "Version": "2012-10-17",
       "Statement": [
           {
               "Effect": "Allow",
               "Principal": {
                   "Service": "lambda.amazonaws.com"
               },
               "Action": "sts:AssumeRole"
           }
       ]
   }
   ```

### 3. Create AWS Glue Jobs
Manually create Glue jobs for `extract.py` and `transform.py` (or automate via CI/CD):
```bash
aws glue create-job \
  --name <extract-job-name> \
  --role arn:aws:iam::<account-id>:role/GlueExecutionRole \
  --command '{"Name": "glueetl", "ScriptLocation": "s3://<bucket-name>/jobs/etl/extract.py"}' \
  --default-arguments '{"--enable-metrics": "true", "--job-language": "python"}' \
  --glue-version "4.0"

aws glue create-job \
  --name <transform-job-name> \
  --role arn:aws:iam::<account-id>:role/GlueExecutionRole \
  --command '{"Name": "glueetl", "ScriptLocation": "s3://<bucket-name>/jobs/etl/transform.py"}' \
  --default-arguments '{"--enable-metrics": "true", "--job-language": "python"}' \
  --glue-version "4.0"
```

### 4. Create AWS Lambda Function
Create the Lambda function for `lambda_function.py`:
```bash
aws lambda create-function \
  --function-name <lambda-function-name> \
  --runtime python3.9 \
  --role arn:aws:iam::<account-id>:role/LambdaExecutionRole \
  --handler lambda_function.lambda_handler \
  --code S3Bucket=<bucket-name>,S3Key=jobs/lambda/lambda_function.zip \
  --environment Variables={STATE_MACHINE_ARN=arn:aws:states:<region>:<account-id>:stateMachine:<state-machine-name>}
```

Add an S3 event trigger:
```bash
aws lambda add-permission \
  --function-name <lambda-function-name> \
  --statement-id s3-trigger \
  --action "lambda:InvokeFunction" \
  --principal s3.amazonaws.com \
  --source-arn arn:aws:s3:::<source-bucket-name>

aws s3api put-bucket-notification-configuration \
  --bucket <source-bucket-name> \
  --notification-configuration '{
      "LambdaFunctionConfigurations": [
          {
              "LambdaFunctionArn": "arn:aws:lambda:<region>:<account-id>:function:<lambda-function-name>",
              "Events": ["s3:ObjectCreated:*"],
              "Filter": {
                  "Key": {
                      "FilterRules": [
                          {"Name": "suffix", "Value": ".csv"}
                      ]
                  }
              }
          }
      ]
  }'
```

### 5. Create Step Functions State Machine
Create the Step Functions state machine (replace with `lab5_stepfunction.json` content):
```bash
aws stepfunctions create-state-machine \
  --name <state-machine-name> \
  --definition file://lab5_stepfunction.json \
  --role-arn arn:aws:iam::<account-id>:role/StepFunctionsExecutionRole
```
**Note**: Share `lab5_stepfunction.json` to ensure correct configuration. The state machine should include states for:
- Running `<extract-job-name>` and `<transform-job-name>` Glue jobs.
- Archiving files to the S3 archived prefix.
- Updating the Glue Data Catalog (e.g., via a Glue Crawler).

### 6. Create Glue Data Catalog Database
Create a database for Athena querying:
```bash
aws glue create-database --database-input '{"Name": "<database-name>"}'
```

## Step 3: Set Up Local Development Environment
1. **Install Dependencies**:
   ```bash
   python -m pip install --upgrade pip
   pip install -r requirements.txt
   ```
2. **Install Testing Tools** (optional for local testing):
   ```bash
   pip install pytest pytest-cov moto
   ```
3. **Test Locally** (if tests are implemented):
   ```bash
   pytest tests/unit/ -v --cov=jobs
   ```

## Step 4: Configure CI/CD Pipeline
1. **Set Up GitHub Secrets**:
   - In your GitHub repository, add the following secrets under Settings > Secrets and variables > Actions:
     - `AWS_ACCESS_KEY_ID`: Your AWS access key.
     - `AWS_SECRET_ACCESS_KEY`: Your AWS secret key.
     - `AWS_ACCOUNT_ID`: Your AWS account ID.
2. **Update `deploy-pipeline.yml`**:
   - Ensure the pipeline references `requirements.txt` and deploys to `s3://<bucket-name>/jobs/`.
   - Uncomment the `unit-tests` and `integration-tests` jobs and implement tests in `tests/unit/` and `tests/integration/`.
3. **Push Changes**:
   ```bash
   git add .
   git commit -m "Initial setup"
   git push origin main
   ```
   The GitHub Actions pipeline will build, test, and deploy scripts to S3.

## Step 5: Upload Sample Data
Upload sample CSV files to the source S3 bucket's raw prefix:
```bash
aws s3 cp sample_products.csv s3://<source-bucket-name>/raw/products/
aws s3 cp sample_orders.csv s3://<source-bucket-name>/raw/orders/
aws s3 cp sample_order_items.csv s3://<source-bucket-name>/raw/order_items/
```

## Step 6: Test the Pipeline
1. **Trigger Pipeline**: Upload a CSV file to the source S3 bucket's raw prefix.
   ```bash
   aws s3 cp test.csv s3://<source-bucket-name>/raw/products/
   ```
2. **Monitor Execution**:
   - Check Step Functions executions in the AWS Console.
   - Verify logs in the S3 logs prefix and CloudWatch.
   - Check Delta tables in the processed S3 prefix and rejected records in the S3 rejected records prefix.
3. **Query with Athena**:
   - After Catalog updates, query tables in the `<database-name>` database using Athena.

## Troubleshooting
- **Bucket Configuration**: Ensure all scripts use the same S3 bucket configuration or clarify the role of the source bucket.
- **Missing Dependencies**: Verify `requirements.txt` compatibility with Glue 4.0 and Lambda Python 3.9 runtime.
- **Step Functions Errors**: Check `lab5_stepfunction.json` for correct Glue job names and ARNs.
- **Glue Data Catalog**: Implement Catalog updates in the transform script or Step Functions if missing.

## Next Steps
- Implement unit and integration tests in `tests/`.
- Share `lab5_stepfunction.json` to finalize orchestration setup.
- Remove Excel processing from `utility.py` if not required.
- Add Glue Data Catalog updates via Spark SQL or a Glue Crawler.
- Move archiving to post-transformation in Step Functions or the transform script.