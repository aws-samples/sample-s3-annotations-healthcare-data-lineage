#!/bin/bash

# ==========================================================
# S3 Annotations Data Lineage Solution - Teardown Script
# ==========================================================
# Safely removes all resources created by the deployment
# Usage: ./teardown.sh [stack-name] [region]

set -e  # Exit on error

# Configuration
STACK_NAME=${1:-"data-lineage-demo"}
AWS_REGION=${2:-"us-east-1"}

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo "========================================="
echo "Data Lineage Solution Teardown"
echo "========================================="
echo "Stack: $STACK_NAME"
echo "Region: $AWS_REGION"
echo ""

# Verify stack exists
if ! aws cloudformation describe-stacks --stack-name "$STACK_NAME" --region "$AWS_REGION" >/dev/null 2>&1; then
    echo -e "${RED}Stack $STACK_NAME not found in region $AWS_REGION${NC}"
    exit 1
fi

echo -e "${YELLOW}WARNING: This will delete all resources and data${NC}"
read -p "Are you sure you want to proceed? (yes/no): " CONFIRM

if [ "$CONFIRM" != "yes" ]; then
    echo "Teardown cancelled"
    exit 0
fi

echo ""

# Get bucket names from stack
echo -e "${YELLOW}[1/4] Retrieving stack outputs...${NC}"

BRONZE_BUCKET=$(aws cloudformation describe-stacks \
    --stack-name "$STACK_NAME" \
    --region "$AWS_REGION" \
    --query 'Stacks[0].Outputs[?OutputKey==`BronzeBucketName`].OutputValue' \
    --output text 2>/dev/null || echo "")

SILVER_BUCKET=$(aws cloudformation describe-stacks \
    --stack-name "$STACK_NAME" \
    --region "$AWS_REGION" \
    --query 'Stacks[0].Outputs[?OutputKey==`SilverBucketName`].OutputValue' \
    --output text 2>/dev/null || echo "")

GOLD_BUCKET=$(aws cloudformation describe-stacks \
    --stack-name "$STACK_NAME" \
    --region "$AWS_REGION" \
    --query 'Stacks[0].Outputs[?OutputKey==`GoldBucketName`].OutputValue' \
    --output text 2>/dev/null || echo "")

SCRIPTS_BUCKET=$(aws cloudformation describe-stacks \
    --stack-name "$STACK_NAME" \
    --region "$AWS_REGION" \
    --query 'Stacks[0].Outputs[?OutputKey==`ScriptsBucketName`].OutputValue' \
    --output text 2>/dev/null || echo "")

echo -e "${GREEN}✓ Stack outputs retrieved${NC}"

# Empty S3 buckets
echo -e "${YELLOW}[2/4] Emptying S3 buckets...${NC}"

for BUCKET in "$BRONZE_BUCKET" "$SILVER_BUCKET" "$GOLD_BUCKET" "$SCRIPTS_BUCKET"; do
    if [ -n "$BUCKET" ] && aws s3 ls "s3://$BUCKET" --region "$AWS_REGION" >/dev/null 2>&1; then
        echo "  Emptying s3://$BUCKET..."
        aws s3 rm "s3://$BUCKET" --recursive --region "$AWS_REGION" --quiet || true
        echo "  ✓ s3://$BUCKET emptied"
    fi
done

echo -e "${GREEN}✓ All buckets emptied${NC}"

# Delete CloudFormation stack
echo -e "${YELLOW}[3/4] Deleting CloudFormation stack...${NC}"

aws cloudformation delete-stack \
    --stack-name "$STACK_NAME" \
    --region "$AWS_REGION"

echo "  Waiting for stack deletion..."
aws cloudformation wait stack-delete-complete \
    --stack-name "$STACK_NAME" \
    --region "$AWS_REGION" 2>/dev/null || true

echo -e "${GREEN}✓ CloudFormation stack deleted${NC}"

# Verify deletion
echo -e "${YELLOW}[4/4] Verifying cleanup...${NC}"

# Check if buckets still exist
for BUCKET in "$BRONZE_BUCKET" "$SILVER_BUCKET" "$GOLD_BUCKET" "$SCRIPTS_BUCKET"; do
    if [ -n "$BUCKET" ] && aws s3 ls "s3://$BUCKET" --region "$AWS_REGION" >/dev/null 2>&1; then
        echo -e "  ${YELLOW}⚠ Warning: s3://$BUCKET still exists${NC}"
        echo "    Run: aws s3 rb s3://$BUCKET --force --region $AWS_REGION"
    fi
done

echo -e "${GREEN}✓ Cleanup verification complete${NC}"

echo ""
echo "========================================="
echo "✓ TEARDOWN COMPLETE"
echo "========================================="
echo ""
echo "All resources for stack '$STACK_NAME' have been removed."
echo ""
