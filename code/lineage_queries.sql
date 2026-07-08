-- ==================================================
-- S3 Annotations Data Lineage Queries
-- Compliance audit queries for HIPAA, GDPR, SOX
-- ==================================================

-- Note: These queries assume S3 Metadata Annotation Tables are enabled
-- and queryable via Athena. Until then, queries use object metadata.

-- ==================================================
-- Query 1: Full Lineage for Specific Object
-- Use case: HIPAA audit - show all transformations
-- ==================================================

-- Using S3 inventory or list objects with metadata
SELECT
  bucket,
  key,
  last_modified,
  size,
  storage_class
FROM s3_inventory_table
WHERE key = 'analytics/patient_360/2026-06-18/part-001.parquet'
ORDER BY last_modified DESC;

-- Alternative: Query S3 Metadata Annotation Tables (when available)
/*
SELECT
  object_key,
  name AS annotation_name,
  JSON_EXTRACT_SCALAR(text_value, '$.lineage_type') AS lineage_type,
  JSON_EXTRACT_SCALAR(text_value, '$.timestamp') AS operation_time,
  JSON_EXTRACT_SCALAR(text_value, '$.operator') AS operator_role
FROM "s3tablescatalog/aws-s3"."b_data-lineage-demo-gold-<ACCOUNT_ID>"."annotation"
WHERE object_key = 'analytics/patient_360/2026-06-18/part-001.parquet'
  AND name = 'lineage'
ORDER BY operation_time;
*/


-- ==================================================
-- Query 2: Data Provenance Chain (Recursive)
-- Use case: Trace object back to source system
-- ==================================================

-- Simplified version - shows lineage chain concept
-- Full recursive query requires annotation tables
/*
WITH RECURSIVE lineage_chain AS (
  -- Start with target object
  SELECT
    object_key,
    JSON_EXTRACT(text_value, '$.source_objects') AS sources,
    JSON_EXTRACT_SCALAR(text_value, '$.source_system') AS origin,
    JSON_EXTRACT_SCALAR(text_value, '$.lineage_type') AS lineage_type,
    1 AS depth
  FROM "s3tablescatalog/aws-s3"."b_data-lineage-demo-gold-<ACCOUNT_ID>"."annotation"
  WHERE object_key = 'analytics/patient_360/2026-06-18/part-001.parquet'
    AND name = 'lineage'

  UNION ALL

  -- Recursively find parent objects
  SELECT
    a.object_key,
    JSON_EXTRACT(a.text_value, '$.source_objects') AS sources,
    JSON_EXTRACT_SCALAR(a.text_value, '$.source_system') AS origin,
    JSON_EXTRACT_SCALAR(a.text_value, '$.lineage_type') AS lineage_type,
    lc.depth + 1
  FROM "s3tablescatalog/aws-s3"."b_data-lineage-demo-bronze-<ACCOUNT_ID>"."annotation" a
  CROSS JOIN UNNEST(CAST(json_parse(lc.sources) AS ARRAY(VARCHAR))) AS t(source_uri)
  WHERE a.object_key = t.source_uri
    AND depth < 10
)
SELECT
  depth,
  lineage_type,
  object_key,
  origin
FROM lineage_chain
ORDER BY depth;
*/


-- ==================================================
-- Query 3: HIPAA Compliance - PHI Access Audit
-- Use case: Who accessed PHI in last 30 days?
-- ==================================================

-- Using CloudTrail logs for S3 access
SELECT
  eventTime,
  userIdentity.principalId AS accessor,
  requestParameters.bucketName AS bucket,
  requestParameters.key AS object_key,
  eventName AS action,
  sourceIPAddress
FROM cloudtrail_logs
WHERE eventSource = 's3.amazonaws.com'
  AND requestParameters.bucketName LIKE 'data-lineage-demo-%'
  AND eventTime >= CURRENT_TIMESTAMP - INTERVAL '30' DAY
  AND (
    requestParameters.key LIKE '%patient%'
    OR requestParameters.key LIKE '%ehr%'
  )
ORDER BY eventTime DESC;


-- ==================================================
-- Query 4: GDPR Article 17 - Right to Erasure
-- Use case: Find ALL objects containing a specific patient's data
-- across the entire data lake (all tiers) for deletion/anonymization
-- ==================================================

-- Search consent annotations for patient ID across all buckets
/*
SELECT
  object_key,
  JSON_EXTRACT_SCALAR(text_value, '$.timestamp') AS ingested_at,
  JSON_EXTRACT_SCALAR(text_value, '$.data_classification') AS classification,
  JSON_EXTRACT_SCALAR(text_value, '$.consent_basis') AS legal_basis,
  JSON_EXTRACT_SCALAR(text_value, '$.retention_period') AS retention,
  JSON_EXTRACT_SCALAR(text_value, '$.s3_uri') AS full_uri
FROM (
  SELECT object_key, text_value FROM "s3tablescatalog/aws-s3"."b_data-lineage-demo-bronze-<ACCOUNT_ID>"."annotation" WHERE name = 'consent'
  UNION ALL
  SELECT object_key, text_value FROM "s3tablescatalog/aws-s3"."b_data-lineage-demo-silver-<ACCOUNT_ID>"."annotation" WHERE name = 'consent'
  UNION ALL
  SELECT object_key, text_value FROM "s3tablescatalog/aws-s3"."b_data-lineage-demo-gold-<ACCOUNT_ID>"."annotation" WHERE name = 'consent'
)
WHERE JSON_EXTRACT_SCALAR(text_value, '$.erasure_eligible') = 'true'
  AND CONTAINS(
    CAST(JSON_EXTRACT(text_value, '$.patient_ids') AS ARRAY(VARCHAR)),
    'MRN1234567'
  )
ORDER BY ingested_at DESC;
*/


-- ==================================================
-- Query 4b: GDPR Article 30 - Processing Records
-- Use case: Summary of all processing activities for a data subject
-- ==================================================

/*
SELECT
  JSON_EXTRACT_SCALAR(lineage.text_value, '$.lineage_type') AS operation,
  JSON_EXTRACT_SCALAR(lineage.text_value, '$.provenance.activity') AS activity,
  JSON_EXTRACT_SCALAR(lineage.text_value, '$.provenance.agent.who') AS processor,
  JSON_EXTRACT_SCALAR(lineage.text_value, '$.timestamp') AS when_processed,
  JSON_EXTRACT_SCALAR(consent.text_value, '$.consent_basis') AS legal_basis,
  lineage.object_key
FROM "s3tablescatalog/aws-s3"."b_data-lineage-demo-bronze-<ACCOUNT_ID>"."annotation" lineage
JOIN "s3tablescatalog/aws-s3"."b_data-lineage-demo-bronze-<ACCOUNT_ID>"."annotation" consent
  ON lineage.object_key = consent.object_key
WHERE lineage.name = 'lineage'
  AND consent.name = 'consent'
  AND CONTAINS(
    CAST(JSON_EXTRACT(consent.text_value, '$.patient_ids') AS ARRAY(VARCHAR)),
    'MRN1234567'
  )
ORDER BY when_processed DESC;
*/


-- ==================================================
-- Query 5: SOX Financial Audit - Source to Report
-- Use case: Trace financial report back to source
-- ==================================================

-- Example showing transformation quality scores
/*
SELECT
  JSON_EXTRACT_SCALAR(text_value, '$.lineage_type') AS operation,
  JSON_EXTRACT_SCALAR(text_value, '$.transformation.job_name') AS job_name,
  JSON_EXTRACT_SCALAR(text_value, '$.input_records') AS input_count,
  JSON_EXTRACT_SCALAR(text_value, '$.output_records') AS output_count,
  JSON_EXTRACT_SCALAR(text_value, '$.quality_score') AS quality_score,
  JSON_EXTRACT_SCALAR(text_value, '$.compliance_validation') AS validation_status,
  JSON_EXTRACT_SCALAR(text_value, '$.timestamp') AS operation_time
FROM "s3tablescatalog/aws-s3"."b_data-lineage-demo-silver-<ACCOUNT_ID>"."annotation"
WHERE name = 'lineage'
  AND JSON_EXTRACT_SCALAR(text_value, '$.lineage_type') = 'transformation'
  AND object_key LIKE 'fhir/patients/%'
ORDER BY operation_time DESC;
*/


-- ==================================================
-- Query 6: Data Quality Metrics
-- Use case: Monitor transformation quality across pipeline
-- ==================================================

/*
SELECT
  JSON_EXTRACT_SCALAR(text_value, '$.transformation.job_name') AS job_name,
  COUNT(*) AS total_runs,
  AVG(CAST(JSON_EXTRACT_SCALAR(text_value, '$.quality_score') AS DOUBLE)) AS avg_quality_score,
  MIN(CAST(JSON_EXTRACT_SCALAR(text_value, '$.quality_score') AS DOUBLE)) AS min_quality_score,
  MAX(CAST(JSON_EXTRACT_SCALAR(text_value, '$.quality_score') AS DOUBLE)) AS max_quality_score,
  SUM(
    CASE
      WHEN JSON_EXTRACT_SCALAR(text_value, '$.compliance_validation') = 'PASSED'
      THEN 1
      ELSE 0
    END
  ) AS passed_validations
FROM "s3tablescatalog/aws-s3"."b_data-lineage-demo-silver-<ACCOUNT_ID>"."annotation"
WHERE name = 'lineage'
  AND JSON_EXTRACT_SCALAR(text_value, '$.lineage_type') = 'transformation'
  AND JSON_EXTRACT_SCALAR(text_value, '$.timestamp') >=
      CAST(CURRENT_DATE - INTERVAL '7' DAY AS VARCHAR)
GROUP BY JSON_EXTRACT_SCALAR(text_value, '$.transformation.job_name')
ORDER BY avg_quality_score DESC;
*/


-- ==================================================
-- Query 7: Compliance Tag Summary
-- Use case: Overview of data classifications
-- ==================================================

/*
SELECT
  JSON_EXTRACT_SCALAR(text_value, '$.data_classification') AS classification,
  JSON_EXTRACT_SCALAR(text_value, '$.lineage_type') AS layer,
  COUNT(*) AS object_count,
  SUM(size) AS total_size_bytes
FROM "s3tablescatalog/aws-s3"."b_data-lineage-demo-bronze-<ACCOUNT_ID>"."annotation"
WHERE name = 'lineage'
GROUP BY
  JSON_EXTRACT_SCALAR(text_value, '$.data_classification'),
  JSON_EXTRACT_SCALAR(text_value, '$.lineage_type')
ORDER BY object_count DESC;
*/


-- ==================================================
-- Query 8: Operator Activity Report
-- Use case: Track which roles/users performed operations
-- ==================================================

/*
SELECT
  JSON_EXTRACT_SCALAR(text_value, '$.operator') AS operator_arn,
  JSON_EXTRACT_SCALAR(text_value, '$.lineage_type') AS operation_type,
  COUNT(*) AS operation_count,
  MIN(JSON_EXTRACT_SCALAR(text_value, '$.timestamp')) AS first_operation,
  MAX(JSON_EXTRACT_SCALAR(text_value, '$.timestamp')) AS last_operation
FROM (
  SELECT text_value FROM "s3tablescatalog/aws-s3"."b_data-lineage-demo-bronze-<ACCOUNT_ID>"."annotation" WHERE name = 'lineage'
  UNION ALL
  SELECT text_value FROM "s3tablescatalog/aws-s3"."b_data-lineage-demo-silver-<ACCOUNT_ID>"."annotation" WHERE name = 'lineage'
  UNION ALL
  SELECT text_value FROM "s3tablescatalog/aws-s3"."b_data-lineage-demo-gold-<ACCOUNT_ID>"."annotation" WHERE name = 'lineage'
) AS all_lineage
GROUP BY
  JSON_EXTRACT_SCALAR(text_value, '$.operator'),
  JSON_EXTRACT_SCALAR(text_value, '$.lineage_type')
ORDER BY operation_count DESC;
*/
