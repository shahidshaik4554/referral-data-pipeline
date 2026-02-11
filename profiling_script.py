import pandas as pd
import os

DATA_PATH = "data"
OUTPUT_PATH = "output"

os.makedirs(OUTPUT_PATH, exist_ok=True)

files = {
    "lead_logs": "lead_logs.csv",
    "user_referrals": "user_referrals.csv",
    "user_referral_logs": "user_referral_logs.csv",
    "user_logs": "user_logs.csv",
    "user_referral_statuses": "user_referral_statuses.csv",
    "referral_rewards": "referral_rewards.csv",
    "paid_transactions": "paid_transactions.csv"
}

profiling_results = []

for table_name, file_name in files.items():
    df = pd.read_csv(f"{DATA_PATH}/{file_name}")
    
    for column in df.columns:
        profiling_results.append({
            "table_name": table_name,
            "column_name": column,
            "data_type": str(df[column].dtype),
            "null_count": df[column].isnull().sum(),
            "distinct_count": df[column].nunique()
        })

profiling_df = pd.DataFrame(profiling_results)

profiling_df.to_csv(f"{OUTPUT_PATH}/data_profiling_report.csv", index=False)

print("✅ Data profiling report generated successfully!")