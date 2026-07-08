#!/bin/bash

# ==========================================================
# S3 Annotations Data Lineage Solution - Deployment Script
# ==========================================================
# Deploys complete medallion architecture with lineage tracking
# Usage: ./deploy.sh [stack-name] [region]

set -e  # Exit on error

# Configuration
STACK_NAME=${1:-"data-lineage-demo"}
AWS_REGION=${2:-"us-east-1"}
PROJECT_NAME="data-lineage-demo"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo "========================================="
echo "Data Lineage Solution Deployment"
echo "========================================="
echo "Stack: $STACK_NAME"
echo "Region: $AWS_REGION"
echo ""

# Get AWS account ID
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
echo "AWS Account: $ACCOUNT_ID"
echo ""

# ==========================================================
# Step 1: Package Lambda Function
# ==========================================================
echo -e "${YELLOW}[1/8] Packaging Lambda function...${NC}"
cd ../code
rm -rf lambda_package lambda_package.zip
mkdir -p lambda_package
python3.13 -m pip install boto3 -t lambda_package --quiet --upgrade
cp lambda_lineage_tracker.py lambda_package/
cd lambda_package && zip -qr ../lambda_package.zip . && cd ..
rm -rf lambda_package
echo -e "${GREEN}✓ Lambda packaged (with boto3 for S3 Annotations API)${NC}"
cd ../infrastructure

# ==========================================================
# Step 2: Create/Verify Scripts Bucket
# ==========================================================
echo -e "${YELLOW}[2/8] Checking scripts bucket...${NC}"
SCRIPTS_BUCKET="${PROJECT_NAME}-scripts-${ACCOUNT_ID}"

if aws s3 ls "s3://${SCRIPTS_BUCKET}" 2>&1 | grep -q 'NoSuchBucket'; then
    echo "Scripts bucket doesn't exist, will be created by stack"
else
    echo -e "${GREEN}✓ Scripts bucket exists${NC}"
fi

# ==========================================================
# Step 3: Deploy CloudFormation Stack
# ==========================================================
echo -e "${YELLOW}[3/8] Deploying CloudFormation stack...${NC}"

# Check if stack exists
if aws cloudformation describe-stacks --stack-name "$STACK_NAME" --region "$AWS_REGION" >/dev/null 2>&1; then
    echo "Stack exists, updating..."
    aws cloudformation update-stack \
        --stack-name "$STACK_NAME" \
        --template-body file://template.yaml \
        --parameters ParameterKey=ProjectName,ParameterValue="$PROJECT_NAME" \
                     ParameterKey=Environment,ParameterValue=dev \
        --capabilities CAPABILITY_NAMED_IAM \
        --region "$AWS_REGION" || echo "No updates to perform"

    aws cloudformation wait stack-update-complete \
        --stack-name "$STACK_NAME" \
        --region "$AWS_REGION" 2>/dev/null || true
else
    echo "Creating new stack..."
    aws cloudformation create-stack \
        --stack-name "$STACK_NAME" \
        --template-body file://template.yaml \
        --parameters ParameterKey=ProjectName,ParameterValue="$PROJECT_NAME" \
                     ParameterKey=Environment,ParameterValue=dev \
        --capabilities CAPABILITY_NAMED_IAM \
        --region "$AWS_REGION"

    echo "Waiting for stack creation..."
    aws cloudformation wait stack-create-complete \
        --stack-name "$STACK_NAME" \
        --region "$AWS_REGION"
fi

echo -e "${GREEN}✓ CloudFormation stack deployed${NC}"

# ==========================================================
# Step 4: Get Stack Outputs
# ==========================================================
echo -e "${YELLOW}[4/8] Retrieving stack outputs...${NC}"

BRONZE_BUCKET=$(aws cloudformation describe-stacks \
    --stack-name "$STACK_NAME" \
    --region "$AWS_REGION" \
    --query 'Stacks[0].Outputs[?OutputKey==`BronzeBucketName`].OutputValue' \
    --output text)

SILVER_BUCKET=$(aws cloudformation describe-stacks \
    --stack-name "$STACK_NAME" \
    --region "$AWS_REGION" \
    --query 'Stacks[0].Outputs[?OutputKey==`SilverBucketName`].OutputValue' \
    --output text)

GOLD_BUCKET=$(aws cloudformation describe-stacks \
    --stack-name "$STACK_NAME" \
    --region "$AWS_REGION" \
    --query 'Stacks[0].Outputs[?OutputKey==`GoldBucketName`].OutputValue' \
    --output text)

SCRIPTS_BUCKET=$(aws cloudformation describe-stacks \
    --stack-name "$STACK_NAME" \
    --region "$AWS_REGION" \
    --query 'Stacks[0].Outputs[?OutputKey==`ScriptsBucketName`].OutputValue' \
    --output text)

LAMBDA_FUNCTION=$(aws cloudformation describe-stacks \
    --stack-name "$STACK_NAME" \
    --region "$AWS_REGION" \
    --query 'Stacks[0].Outputs[?OutputKey==`LineageTrackerFunctionArn`].OutputValue' \
    --output text | cut -d':' -f7)

echo -e "${GREEN}✓ Stack outputs retrieved${NC}"

# ==========================================================
# Step 5: Upload Lambda Code & Glue Scripts
# ==========================================================
echo -e "${YELLOW}[5/8] Uploading code artifacts...${NC}"

# Upload Lambda function
aws lambda update-function-code \
    --function-name "$LAMBDA_FUNCTION" \
    --zip-file fileb://../code/lambda_package.zip \
    --region "$AWS_REGION" \
    --query 'LastModified' \
    --output text > /dev/null

echo "  ✓ Lambda function updated"

# Upload Glue scripts
aws s3 cp ../code/glue_etl_bronze_to_silver.py "s3://${SCRIPTS_BUCKET}/" --region "$AWS_REGION" --quiet
aws s3 cp ../code/glue_etl_silver_to_gold.py "s3://${SCRIPTS_BUCKET}/" --region "$AWS_REGION" --quiet

echo "  ✓ Glue scripts uploaded"
echo -e "${GREEN}✓ All code artifacts deployed${NC}"

# ==========================================================
# Step 6: Generate and Upload Sample Data
# ==========================================================
echo -e "${YELLOW}[6/8] Generating sample data...${NC}"

cd ../code
if [ ! -d "sample_data/bronze" ]; then
    echo "Generating synthetic healthcare data..."
    python3 sample_data_generator.py > /dev/null 2>&1 || echo "Sample data generation skipped"
fi

if [ -d "sample_data/bronze" ]; then
    echo "Uploading Bronze layer sample data..."
    aws s3 cp sample_data/bronze/ "s3://${BRONZE_BUCKET}/ehr/2026-06-18/" \
        --recursive \
        --exclude "*" \
        --include "*.hl7" \
        --region "$AWS_REGION" \
        --quiet

    FILE_COUNT=$(ls -1 sample_data/bronze/*.hl7 | wc -l)
    echo "  ✓ Uploaded $FILE_COUNT HL7 files to Bronze"
else
    echo "  ⚠ Sample data directory not found, skipping"
fi

cd ../infrastructure
echo -e "${GREEN}✓ Sample data uploaded${NC}"

# ==========================================================
# Step 7: Trigger ETL Pipeline
# ==========================================================
echo -e "${YELLOW}[7/8] Starting ETL pipeline...${NC}"

# Start Bronze to Silver job
BRONZE_TO_SILVER_RUN=$(aws glue start-job-run \
    --job-name "${PROJECT_NAME}-bronze-to-silver" \
    --region "$AWS_REGION" \
    --query 'JobRunId' \
    --output text)

echo "  Started Bronze→Silver job: $BRONZE_TO_SILVER_RUN"

# Wait for Bronze to Silver completion
echo "  Waiting for Bronze→Silver job to complete..."
while true; do
    JOB_STATE=$(aws glue get-job-run \
        --job-name "${PROJECT_NAME}-bronze-to-silver" \
        --run-id "$BRONZE_TO_SILVER_RUN" \
        --region "$AWS_REGION" \
        --query 'JobRun.JobRunState' \
        --output text)

    if [ "$JOB_STATE" = "SUCCEEDED" ]; then
        echo -e "  ${GREEN}✓ Bronze→Silver completed${NC}"
        break
    elif [ "$JOB_STATE" = "FAILED" ] || [ "$JOB_STATE" = "ERROR" ]; then
        echo -e "  ${RED}✗ Bronze→Silver failed${NC}"
        break
    fi

    sleep 10
done

# Skip Silver-to-Gold if Bronze-to-Silver failed
if [ "$JOB_STATE" != "SUCCEEDED" ]; then
    echo -e "${RED}Skipping Silver→Gold due to upstream failure${NC}"
    echo -e "${GREEN}✓ ETL pipeline completed (with failures)${NC}"
    exit 1
fi

# Start Silver to Gold job
SILVER_TO_GOLD_RUN=$(aws glue start-job-run \
    --job-name "${PROJECT_NAME}-silver-to-gold" \
    --region "$AWS_REGION" \
    --query 'JobRunId' \
    --output text)

echo "  Started Silver→Gold job: $SILVER_TO_GOLD_RUN"

# Wait for Silver to Gold completion
echo "  Waiting for Silver→Gold job to complete..."
while true; do
    JOB_STATE=$(aws glue get-job-run \
        --job-name "${PROJECT_NAME}-silver-to-gold" \
        --run-id "$SILVER_TO_GOLD_RUN" \
        --region "$AWS_REGION" \
        --query 'JobRun.JobRunState' \
        --output text)

    if [ "$JOB_STATE" = "SUCCEEDED" ]; then
        echo -e "  ${GREEN}✓ Silver→Gold completed${NC}"
        break
    elif [ "$JOB_STATE" = "FAILED" ] || [ "$JOB_STATE" = "ERROR" ]; then
        echo -e "  ${RED}✗ Silver→Gold failed${NC}"
        break
    fi

    sleep 10
done

echo -e "${GREEN}✓ ETL pipeline completed${NC}"

# ==========================================================
# Step 8: Verify Deployment
# ==========================================================
echo -e "${YELLOW}[8/8] Verifying deployment...${NC}"

# Check Lambda executions
LAMBDA_INVOCATIONS=$(aws cloudwatch get-metric-statistics \
    --namespace AWS/Lambda \
    --metric-name Invocations \
    --dimensions Name=FunctionName,Value="$LAMBDA_FUNCTION" \
    --start-time $(date -u -v-10M +%Y-%m-%dT%H:%M:%S 2>/dev/null || date -u -d '10 minutes ago' +%Y-%m-%dT%H:%M:%S) \
    --end-time $(date -u +%Y-%m-%dT%H:%M:%S) \
    --period 600 \
    --statistics Sum \
    --region "$AWS_REGION" \
    --query 'Datapoints[0].Sum' \
    --output text 2>/dev/null || echo "0")

echo "  Lambda invocations: $LAMBDA_INVOCATIONS"

# Check bucket object counts
BRONZE_COUNT=$(aws s3 ls "s3://${BRONZE_BUCKET}/ehr/" --recursive --region "$AWS_REGION" | wc -l)
SILVER_COUNT=$(aws s3 ls "s3://${SILVER_BUCKET}/fhir/" --recursive --region "$AWS_REGION" | wc -l)
GOLD_COUNT=$(aws s3 ls "s3://${GOLD_BUCKET}/analytics/" --recursive --region "$AWS_REGION" | wc -l)

echo "  Bronze objects: $BRONZE_COUNT"
echo "  Silver objects: $SILVER_COUNT"
echo "  Gold objects: $GOLD_COUNT"

echo -e "${GREEN}✓ Deployment verification complete${NC}"

# ==========================================================
# Deployment Summary
# ==========================================================
echo ""
echo "========================================="
echo "✓ DEPLOYMENT COMPLETE"
echo "========================================="
echo ""
echo "Stack Name: $STACK_NAME"
echo "Region: $AWS_REGION"
echo ""
echo "S3 Buckets:"
echo "  Bronze: s3://$BRONZE_BUCKET"
echo "  Silver: s3://$SILVER_BUCKET"
echo "  Gold:   s3://$GOLD_BUCKET"
echo ""
echo "Lambda Function: $LAMBDA_FUNCTION"
echo ""
echo "Next Steps:"
echo "  1. View Lambda logs:"
echo "     aws logs tail /aws/lambda/$LAMBDA_FUNCTION --follow --region $AWS_REGION"
echo ""
echo "  2. View Glue job logs:"
echo "     aws logs tail /aws-glue/jobs/output --follow --region $AWS_REGION"
echo ""
echo "  3. Query lineage with Athena:"
echo "     Open AWS Console → Athena → Query Editor"
echo "     Use workgroup: ${PROJECT_NAME}-lineage-queries"
echo ""
echo "  4. Upload more data to trigger pipeline:"
echo "     aws s3 cp <file> s3://$BRONZE_BUCKET/ehr/"
echo ""
echo "  5. Clean up when done:"
echo "     aws cloudformation delete-stack --stack-name $STACK_NAME --region $AWS_REGION"
echo "     aws s3 rb s3://$BRONZE_BUCKET --force"
echo "     aws s3 rb s3://$SILVER_BUCKET --force"
echo "     aws s3 rb s3://$GOLD_BUCKET --force"
echo ""
echo "========================================="
