# Infrastructure Deployment Guide

## Prerequisites

- AWS CLI configured with credentials
- Python 3.13+ with boto3, faker
- Bash shell
- Sufficient AWS permissions:
  - CloudFormation
  - S3
  - Lambda
  - Glue
  - EventBridge
  - IAM
  - Athena
  - CloudWatch Logs

## Quick Start

### Deploy Everything

```bash
cd infrastructure
./deploy.sh [stack-name] [region]
```

Default values:
- Stack name: `data-lineage-demo`
- Region: `us-east-1`

Example with custom values:
```bash
./deploy.sh my-lineage-stack us-west-2
```

### What Gets Deployed

1. **S3 Buckets**
   - Bronze layer (raw ingestion)
   - Silver layer (cleaned/transformed)
   - Gold layer (analytics-ready)
   - Scripts bucket (Glue ETL scripts)
   - Athena results bucket

2. **Lambda Function**
   - Lineage tracker for ingestion events
   - Triggered by EventBridge on S3 PutObject
   - Writes lineage annotations

3. **Glue ETL Jobs**
   - Bronze→Silver: HL7 to FHIR transformation
   - Silver→Gold: Patient 360 aggregation
   - Both track transformation lineage

4. **EventBridge Rules**
   - S3 object created events → Lambda
   - Automatic lineage tracking

5. **Athena Workgroup**
   - Pre-configured for lineage queries
   - Output to dedicated results bucket

6. **IAM Roles**
   - Lambda execution role with annotation permissions
   - Glue service role with S3 and annotation access

### Deployment Steps

The `deploy.sh` script performs these steps automatically:

1. Packages Lambda function code
2. Creates/updates CloudFormation stack
3. Uploads Lambda code and Glue scripts
4. Generates and uploads sample healthcare data
5. Triggers ETL pipeline
6. Verifies deployment

Expected duration: **5-10 minutes**

## Manual Deployment

### 1. Deploy CloudFormation Stack

```bash
aws cloudformation create-stack \
  --stack-name data-lineage-demo \
  --template-body file://template.yaml \
  --parameters ParameterKey=ProjectName,ParameterValue=data-lineage-demo \
               ParameterKey=Environment,ParameterValue=dev \
  --capabilities CAPABILITY_NAMED_IAM \
  --region us-east-1

# Wait for completion
aws cloudformation wait stack-create-complete \
  --stack-name data-lineage-demo \
  --region us-east-1
```

### 2. Upload Lambda Code

```bash
cd ../code
zip lambda_package.zip lambda_lineage_tracker.py

aws lambda update-function-code \
  --function-name data-lineage-demo-lineage-tracker \
  --zip-file fileb://lambda_package.zip \
  --region us-east-1
```

### 3. Upload Glue Scripts

```bash
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)

aws s3 cp glue_etl_bronze_to_silver.py \
  s3://data-lineage-demo-scripts-${ACCOUNT_ID}/ \
  --region us-east-1

aws s3 cp glue_etl_silver_to_gold.py \
  s3://data-lineage-demo-scripts-${ACCOUNT_ID}/ \
  --region us-east-1
```

### 4. Upload Sample Data

```bash
python sample_data_generator.py

aws s3 cp sample_data/bronze/ \
  s3://data-lineage-demo-bronze-${ACCOUNT_ID}/ehr/2026-06-18/ \
  --recursive \
  --region us-east-1
```

### 5. Run ETL Jobs

```bash
# Bronze to Silver
aws glue start-job-run \
  --job-name data-lineage-demo-bronze-to-silver \
  --region us-east-1

# Silver to Gold (after Bronze completes)
aws glue start-job-run \
  --job-name data-lineage-demo-silver-to-gold \
  --region us-east-1
```

## Verification

### Check Lambda Executions

```bash
aws logs tail /aws/lambda/data-lineage-demo-lineage-tracker \
  --follow \
  --region us-east-1
```

### Check Glue Job Status

```bash
aws glue get-job-runs \
  --job-name data-lineage-demo-bronze-to-silver \
  --max-results 1 \
  --region us-east-1
```

### Verify Data in Buckets

```bash
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)

# Bronze layer
aws s3 ls s3://data-lineage-demo-bronze-${ACCOUNT_ID}/ehr/ --recursive

# Silver layer
aws s3 ls s3://data-lineage-demo-silver-${ACCOUNT_ID}/fhir/ --recursive

# Gold layer
aws s3 ls s3://data-lineage-demo-gold-${ACCOUNT_ID}/analytics/ --recursive
```

### Query Lineage with Athena

```bash
# Open AWS Console → Athena
# Select workgroup: data-lineage-demo-lineage-queries
# Run queries from code/lineage_queries.sql
```

## Teardown

### Automated Cleanup

```bash
cd infrastructure
./teardown.sh [stack-name] [region]
```

This will:
1. Empty all S3 buckets
2. Delete CloudFormation stack
3. Remove all resources

### Manual Cleanup

```bash
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)

# Empty buckets
aws s3 rm s3://data-lineage-demo-bronze-${ACCOUNT_ID} --recursive
aws s3 rm s3://data-lineage-demo-silver-${ACCOUNT_ID} --recursive
aws s3 rm s3://data-lineage-demo-gold-${ACCOUNT_ID} --recursive
aws s3 rm s3://data-lineage-demo-scripts-${ACCOUNT_ID} --recursive

# Delete stack
aws cloudformation delete-stack \
  --stack-name data-lineage-demo \
  --region us-east-1
```

## Cost Estimate

For demo scale (1000 patient records):

| Service | Monthly Cost |
|---------|--------------|
| S3 Storage (1GB) | $0.02 |
| S3 Requests | $0.01 |
| Lambda (10 invocations/day) | $0.00 |
| Glue (2 DPU-hours/day) | $1.20 |
| Athena (scanned data) | $0.05 |
| CloudWatch Logs | $0.05 |
| **Total** | **~$1.33/month** |

Production scale (10M objects, 100k transformations/month):
- S3 Annotations: ~$3/month
- Glue ETL: ~$50/month
- Total: ~$53/month

## Troubleshooting

### Lambda not triggering

Check EventBridge rule:
```bash
aws events list-rules --region us-east-1 | grep s3-object-created
```

Verify Lambda has EventBridge permission:
```bash
aws lambda get-policy \
  --function-name data-lineage-demo-lineage-tracker \
  --region us-east-1
```

### Glue job fails

Check error logs:
```bash
aws logs tail /aws-glue/jobs/error --region us-east-1
```

Verify IAM role permissions:
```bash
aws iam get-role-policy \
  --role-name data-lineage-demo-glue-service-role \
  --policy-name S3FullAccess
```

### Empty output files

Check Glue job logs for record counts:
```bash
aws logs tail /aws-glue/jobs/output --region us-east-1 | grep "Input records"
```

Verify source data exists:
```bash
aws s3 ls s3://data-lineage-demo-bronze-${ACCOUNT_ID}/ehr/ --recursive
```

## Parameters

### CloudFormation Parameters

- **ProjectName**: Base name for all resources (default: `data-lineage-demo`)
- **Environment**: Deployment environment (default: `dev`, options: dev/staging/prod)

### Script Parameters

```bash
./deploy.sh <stack-name> <region>
```

- `stack-name`: CloudFormation stack name
- `region`: AWS region (must support Glue, EventBridge, Athena)

## Security Considerations

- All S3 buckets have encryption enabled (AES256)
- Versioning enabled on all data buckets
- Public access blocked on all buckets
- IAM roles follow least-privilege principle
- CloudTrail logs all annotation operations
- Lambda execution logs retained for 7 days

## Next Steps

After deployment:

1. Review CloudWatch Logs for lineage tracking
2. Run Athena queries for compliance audits
3. Upload production data to Bronze bucket
4. Configure retention policies per compliance requirements
5. Set up alerting for failed transformations
6. Enable S3 Metadata Annotation Tables (when available)
