# pipeline_etl.py

"""
Simulates a Databricks-style ETL pipeline that processes pharmacy claims data from raw to refined state.
"""

import pandas as pd

# Load raw claims
df = pd.read_csv("claims_raw.csv", parse_dates=["claim_date"])

# Step 1: Filter out rejected claims
df_filtered = df[df["claim_status"] == "Paid"]

# Step 2: Standardize column names
df_filtered.columns = [col.lower() for col in df_filtered.columns]

# Step 3: Derive claim_month and flag high-cost claims
df_filtered["claim_month"] = df_filtered["claim_date"].dt.to_period("M").astype(str)
df_filtered["high_cost_flag"] = df_filtered["claim_cost"] > 100

# Step 4: Group summary
summary = df_filtered.groupby(["claim_month", "drug_code"]).agg(
    total_claims=("claim_id", "count"),
    total_cost=("claim_cost", "sum"),
    avg_cost=("claim_cost", "mean"),
    high_cost_count=("high_cost_flag", "sum")
).reset_index()

# Output final dataset
summary.to_csv("claims_summary.csv", index=False)
print("ETL pipeline executed successfully. Output saved to claims_summary.csv.")
