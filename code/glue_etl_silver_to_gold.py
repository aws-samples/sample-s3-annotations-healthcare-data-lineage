"""
AWS Glue ETL Job: Silver to Gold Layer Aggregation

Creates analytics-ready Patient 360 views from FHIR resources.
Reads transformation lineage from Silver, writes aggregation lineage to Gold.
"""

import sys
import json
import boto3
from datetime import datetime, timezone
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, count, avg, sum as spark_sum, lit, from_json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType

# Initialize clients
s3_client = boto3.client('s3')
sts_client = boto3.client('sts')

# Get job parameters
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'source_bucket',
    'target_bucket',
    'database_name'
])

source_bucket = args['source_bucket']
target_bucket = args['target_bucket']
database_name = args['database_name']

# Initialize Glue context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

print(f"=== Silver to Gold ETL Job ===")
print(f"Source bucket: {source_bucket}")
print(f"Target bucket: {target_bucket}")


def get_operator_arn() -> str:
    """Get ARN of current Glue job execution role."""
    try:
        identity = sts_client.get_caller_identity()
        return identity['Arn']
    except Exception as e:
        print(f"Failed to get caller identity: {e}")
        return "arn:aws:iam::unknown:role/unknown"


def write_aggregation_lineage(
    target_bucket: str,
    target_key: str,
    source_objects: list,
    job_run_id: str,
    dimensions: list,
    measures: list,
    patient_ids: list = None
) -> bool:
    """
    Write aggregation lineage annotation to Gold object.

    Args:
        target_bucket: Target S3 bucket
        target_key: Target S3 key
        source_objects: List of source S3 URIs
        job_run_id: Glue job run ID
        dimensions: List of dimension columns
        measures: List of measure columns

    Returns:
        True if successful
    """
    timestamp = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
    operator_arn = get_operator_arn()

    lineage_data = {
        "lineage_type": "aggregation",
        "provenance": {
            "target": f"s3://{target_bucket}/{target_key}",
            "recorded": timestamp,
            "activity": "aggregation",
            "agent": {"who": operator_arn, "role": "aggregator"},
            "entity": {"role": "derivation", "what": source_objects}
        },
        "source_objects": source_objects,
        "aggregation": {
            "type": "patient_360",
            "dimensions": dimensions,
            "measures": measures
        },
        "timestamp": timestamp,
        "operator": operator_arn,
        "retention_policy": "7_years",
        "access_tier": "gold",
        "glue_version": "5.0",
        "job_name": args['JOB_NAME'],
        "job_run_id": job_run_id
    }

    try:
        annotation_json = json.dumps(lineage_data, indent=2)

        s3_client.put_object_annotation(
            Bucket=target_bucket,
            Key=target_key,
            AnnotationName='lineage',
            AnnotationPayload=annotation_json.encode('utf-8')
        )

        print(f"✓ Aggregation lineage written to s3://{target_bucket}/{target_key}")

        # Write consent annotation (propagate patient tracking to Gold)
        consent_data = {
            "consent_type": "data_subject_tracking",
            "timestamp": timestamp,
            "data_classification": "PHI",
            "patient_ids": patient_ids or [],
            "consent_basis": "legitimate_interest",
            "retention_period": "7_years",
            "erasure_eligible": True,
            "s3_uri": f"s3://{target_bucket}/{target_key}"
        }
        s3_client.put_object_annotation(
            Bucket=target_bucket,
            Key=target_key,
            AnnotationName='consent',
            AnnotationPayload=json.dumps(consent_data, indent=2).encode('utf-8')
        )
        print(f"✓ Consent annotation written to s3://{target_bucket}/{target_key}")

        return True

    except Exception as e:
        print(f"✗ Failed to write aggregation lineage: {e}")
        return False


# Main ETL process
try:
    # Read FHIR patients from Silver bucket
    input_path = f"s3://{source_bucket}/fhir/patients/"
    print(f"Reading from: {input_path}")

    # Define FHIR Patient schema (simplified)
    patient_schema = StructType([
        StructField("resourceType", StringType(), True),
        StructField("id", StringType(), True),
        StructField("gender", StringType(), True),
        StructField("birthDate", StringType(), True),
        StructField("identifier", ArrayType(StructType([
            StructField("system", StringType(), True),
            StructField("value", StringType(), True)
        ])), True),
        StructField("name", ArrayType(StructType([
            StructField("family", StringType(), True),
            StructField("given", ArrayType(StringType()), True)
        ])), True)
    ])

    # Read JSON text files and parse (recursiveFileLookup handles subdirectories)
    raw_df = spark.read.option("recursiveFileLookup", "true").text(input_path)
    raw_count = raw_df.count()
    print(f"Raw text rows read: {raw_count}")
    if raw_count > 0:
        print(f"Sample row: {raw_df.first().value[:100]}")

    fhir_df = raw_df.select(from_json(col("value"), patient_schema).alias("patient"))
    parsed_count = fhir_df.filter(col("patient").isNotNull()).count()
    print(f"Successfully parsed rows: {parsed_count}")

    # Extract patient attributes
    patients_df = fhir_df.filter(col("patient").isNotNull()).select(
        col("patient.id").alias("patient_id"),
        col("patient.gender").alias("gender"),
        col("patient.birthDate").alias("birth_date"),
        col("patient.identifier")[0]["value"].alias("mrn"),
        col("patient.name")[0]["family"].alias("family_name"),
        col("patient.name")[0]["given"][0].alias("given_name")
    )

    input_count = patients_df.count()
    print(f"Input patient records: {input_count}")

    # Create Patient 360 aggregation
    # Group by demographics and calculate metrics
    patient_360 = patients_df.groupBy("gender").agg(
        count("patient_id").alias("total_patients"),
        count("mrn").alias("patients_with_mrn")
    )

    # Add metadata columns
    patient_360_enriched = patient_360.withColumn(
        "created_date", lit(datetime.now().strftime('%Y-%m-%d'))
    ).withColumn(
        "data_tier", lit("gold")
    ).withColumn(
        "compliance_status", lit("HIPAA_COMPLIANT")
    )

    output_count = patient_360_enriched.count()
    print(f"Output aggregated records: {output_count}")

    # Write to Gold bucket as Parquet
    output_path = f"s3://{target_bucket}/analytics/patient_360/{datetime.now().strftime('%Y-%m-%d')}/"
    print(f"Writing to: {output_path}")

    patient_360_enriched.coalesce(1).write \
        .mode("overwrite") \
        .parquet(output_path)

    # Extract distinct patient IDs from source data for consent tracking
    patient_id_list = []
    try:
        pid_rows = patients_df.select("patient_id").distinct().collect()
        patient_id_list = [row.patient_id for row in pid_rows if row.patient_id]
    except Exception as e:
        print(f"Warning: Could not extract patient IDs: {e}")

    # Get written file path
    written_files = spark.sparkContext.wholeTextFiles(output_path).keys().collect()
    if written_files:
        # Find .parquet file
        parquet_files = [f for f in written_files if f.endswith('.parquet')]
        if parquet_files:
            output_key = parquet_files[0].replace(f"s3://{target_bucket}/", "")

            # Write aggregation lineage
            source_objects = [f"s3://{source_bucket}/fhir/patients/"]
            job_run_id = args.get('JOB_RUN_ID', 'local_test')

            dimensions = ["gender", "created_date"]
            measures = ["total_patients", "patients_with_mrn"]

            write_aggregation_lineage(
                target_bucket=target_bucket,
                target_key=output_key,
                source_objects=source_objects,
                job_run_id=job_run_id,
                dimensions=dimensions,
                measures=measures,
                patient_ids=patient_id_list
            )

    print("=== ETL Job Complete ===")
    print(f"Aggregated {input_count} patients into {output_count} demographic groups")

    job.commit()

except Exception as e:
    print(f"ETL Job Failed: {e}")
    raise
