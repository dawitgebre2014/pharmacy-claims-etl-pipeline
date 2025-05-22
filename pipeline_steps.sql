-- pipeline_steps.sql

-- Step 1: Filter to only paid claims
WITH paid_claims AS (
    SELECT *
    FROM claims_raw
    WHERE claim_status = 'Paid'
),

-- Step 2: Add derived columns (claim_month, high_cost_flag)
derived_claims AS (
    SELECT *,
           FORMAT(claim_date, 'yyyy-MM') AS claim_month,
           CASE WHEN claim_cost > 100 THEN 1 ELSE 0 END AS high_cost_flag
    FROM paid_claims
)

-- Step 3: Aggregate summary
SELECT 
    claim_month,
    drug_code,
    COUNT(claim_id) AS total_claims,
    SUM(claim_cost) AS total_cost,
    AVG(claim_cost) AS avg_cost,
    SUM(high_cost_flag) AS high_cost_count
FROM derived_claims
GROUP BY claim_month, drug_code
ORDER BY claim_month, drug_code;
