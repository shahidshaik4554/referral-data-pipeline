import pandas as pd
import os

# ---------------------------------------------------
# 1️⃣ Load Data
# ---------------------------------------------------

DATA_PATH = "data"
OUTPUT_PATH = "output"

os.makedirs(OUTPUT_PATH, exist_ok=True)

lead_logs = pd.read_csv(f"{DATA_PATH}/lead_logs.csv")
user_referrals = pd.read_csv(f"{DATA_PATH}/user_referrals.csv")
user_referral_logs = pd.read_csv(f"{DATA_PATH}/user_referral_logs.csv")
user_logs = pd.read_csv(f"{DATA_PATH}/user_logs.csv")
user_referral_statuses = pd.read_csv(f"{DATA_PATH}/user_referral_statuses.csv")
referral_rewards = pd.read_csv(f"{DATA_PATH}/referral_rewards.csv")
paid_transactions = pd.read_csv(f"{DATA_PATH}/paid_transactions.csv")

# ---------------------------------------------------
# 2️⃣ Convert Datetime Columns
# ---------------------------------------------------

user_referrals["referral_at"] = pd.to_datetime(user_referrals["referral_at"], utc=True)
user_referrals["updated_at"] = pd.to_datetime(user_referrals["updated_at"], utc=True)

user_referral_logs["created_at"] = pd.to_datetime(user_referral_logs["created_at"], utc=True)
lead_logs["created_at"] = pd.to_datetime(lead_logs["created_at"], utc=True)
paid_transactions["transaction_at"] = pd.to_datetime(paid_transactions["transaction_at"], utc=True)

# ---------------------------------------------------
# 3️⃣ Remove Duplicates
# ---------------------------------------------------

user_referrals.drop_duplicates(inplace=True)
user_referral_logs.drop_duplicates(inplace=True)
user_logs.drop_duplicates(inplace=True)
lead_logs.drop_duplicates(inplace=True)

# ---------------------------------------------------
# 4️⃣ Handle Log Tables (Keep Latest Record)
# ---------------------------------------------------

# Referral logs (latest per referral)
valid_referrals = user_referrals["referral_id"].unique()

user_referral_logs_filtered = user_referral_logs[
    user_referral_logs["user_referral_id"].isin(valid_referrals)
]

user_referral_logs_latest = (
    user_referral_logs_filtered
    .sort_values(["user_referral_id", "created_at"], ascending=[True, False])
    .drop_duplicates(subset=["user_referral_id"], keep="first")
)

# User logs (latest per user)
user_logs_sorted = user_logs.sort_values(
    by=["user_id", "membership_expired_date"],
    ascending=[True, False]
)

user_logs_latest = user_logs_sorted.drop_duplicates(
    subset=["user_id"], keep="first"
)

user_logs_latest_clean = user_logs_latest[[
    "user_id",
    "name",
    "phone_number",
    "homeclub",
    "timezone_homeclub",
    "membership_expired_date",
    "is_deleted"
]].rename(columns={
    "name": "referrer_name",
    "phone_number": "referrer_phone_number",
    "homeclub": "referrer_homeclub",
    "timezone_homeclub": "referrer_timezone"
})

# Lead logs (latest per lead)
valid_leads = user_referrals["referee_id"].dropna().unique()

lead_logs_filtered = lead_logs[
    lead_logs["lead_id"].isin(valid_leads)
]

lead_logs_latest = (
    lead_logs_filtered
    .sort_values(["lead_id", "created_at"], ascending=[True, False])
    .drop_duplicates(subset=["lead_id"], keep="first")
)

lead_logs_latest_clean = lead_logs_latest[[
    "lead_id",
    "source_category",
    "preferred_location",
    "timezone_location",
    "current_status"
]]

# ---------------------------------------------------
# 5️⃣ Build Master Dataset
# ---------------------------------------------------

master_df = user_referrals.copy()

master_df = master_df.merge(
    user_referral_logs_latest,
    left_on="referral_id",
    right_on="user_referral_id",
    how="left"
)

master_df = master_df.merge(
    user_referral_statuses,
    left_on="user_referral_status_id",
    right_on="id",
    how="left"
)

master_df = master_df.merge(
    referral_rewards,
    left_on="referral_reward_id",
    right_on="id",
    how="left"
)

master_df = master_df.merge(
    paid_transactions,
    on="transaction_id",
    how="left"
)

master_df = master_df.merge(
    user_logs_latest_clean,
    left_on="referrer_id",
    right_on="user_id",
    how="left"
)

master_df = master_df.merge(
    lead_logs_latest_clean,
    left_on="referee_id",
    right_on="lead_id",
    how="left"
)

# ---------------------------------------------------
# 6️⃣ Create referral_source_category
# ---------------------------------------------------

def get_source_category(row):
    if row["referral_source"] == "User Sign Up":
        return "Online"
    elif row["referral_source"] == "Draft Transaction":
        return "Offline"
    elif row["referral_source"] == "Lead":
        return row["source_category"]
    else:
        return None

master_df["referral_source_category"] = master_df.apply(get_source_category, axis=1)

# ---------------------------------------------------
# 7️⃣ Fraud Detection Logic
# ---------------------------------------------------

def is_valid(row):

    # Valid Condition 2
    if row["description"] in ["Menunggu", "Tidak Berhasil"] and (
        pd.isna(row["reward_value"]) or row["reward_value"] == 0
    ):
        return True

    try:
        cond1 = (
            row["reward_value"] > 0
            and row["description"] == "Berhasil"
            and pd.notna(row["transaction_id"])
            and row["transaction_status"] == "PAID"
            and row["transaction_type"] == "NEW"
            and row["transaction_at"] > row["referral_at"]
            and row["transaction_at"].month == row["referral_at"].month
            and row["membership_expired_date"] >= row["transaction_at"].date()
            and row["is_deleted"] == False
            and row["is_reward_granted"] == True
        )
        return cond1
    except:
        return False


master_df["is_business_logic_valid"] = master_df.apply(is_valid, axis=1)

# ---------------------------------------------------
# 8️⃣ Final Report
# ---------------------------------------------------

final_report = master_df[[
    "referral_id",
    "referral_source",
    "referral_source_category",
    "referral_at",
    "referrer_id",
    "referrer_name",
    "referrer_phone_number",
    "referrer_homeclub",
    "referee_id",
    "referee_name",
    "referee_phone",
    "description",
    "reward_value",
    "transaction_id",
    "transaction_status",
    "transaction_at",
    "transaction_location",
    "transaction_type",
    "updated_at",
    "is_business_logic_valid"
]].rename(columns={
    "description": "referral_status",
    "reward_value": "num_reward_days"
})

final_report.to_csv(f"{OUTPUT_PATH}/referral_report.csv", index=False)

print("✅ Final report generated successfully!")
print(f"Total rows: {final_report.shape[0]}")