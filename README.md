# Referral Program Data Engineering Pipeline

## 📌 Project Overview

This project implements an end-to-end data engineering pipeline for a referral reward program.

The objective is to:

- Load and profile multiple source datasets
- Clean and standardize data
- Handle one-to-many log tables correctly
- Join all tables into a unified master dataset
- Implement business validation rules
- Detect invalid or potentially fraudulent referral rewards
- Generate a final validated report
- Containerize the solution using Docker

The final output contains **46 referral records**, as required.

---

## 🏢 Business Context

The referral program allows existing users (referrers) to invite new users (referees).  
Referrers receive rewards when referral conditions are satisfied.

This pipeline ensures:

- Only valid rewards are granted
- Business rules are enforced
- Fraudulent or invalid rewards are detected

---

## 📊 Data Sources

The following tables are used:

- lead_logs
- user_referrals
- user_referral_logs
- user_logs
- user_referral_statuses
- referral_rewards
- paid_transactions

Each table was profiled for:
- Null count
- Distinct value count
- Data types

Profiling results are included in the submission.

---

## 🔄 Data Processing Steps

### 1️⃣ Data Loading
All CSV files are loaded using Pandas.

### 2️⃣ Data Profiling
For each table:
- Null count calculated
- Distinct value count calculated
- Data types validated

### 3️⃣ Data Cleaning
- Duplicate records removed
- Datetime columns converted to UTC
- String formatting standardized
- Null handling performed carefully

### 4️⃣ Handling Log Tables

The following tables contain historical log data:

- user_referral_logs
- user_logs
- lead_logs

To prevent row duplication (1-to-many joins):

- Only the latest record per entity was retained.
- Referential integrity was preserved.

Final master dataset contains exactly **46 rows**.

### 5️⃣ Master Dataset Creation

All cleaned tables were merged using controlled LEFT JOIN operations.

Row count was validated after each join to prevent duplication or data loss.

---

## 🏷 referral_source_category Logic

Implemented as:

- "User Sign Up" → Online
- "Draft Transaction" → Offline
- "Lead" → Uses lead source_category

---

## 🚨 Fraud Detection Logic

A new column:
is_business_logic_valid
was created.

### A referral reward is considered VALID if:

- Reward value > 0
- Referral status = "Berhasil"
- Transaction exists
- Transaction status = "PAID"
- Transaction type = "NEW"
- Transaction occurs after referral creation
- Transaction occurs in the same month
- Membership not expired
- Account not deleted
- Reward granted

OR

- Referral status is "Menunggu" or "Tidak Berhasil"
- No reward value assigned

All other cases are marked as invalid.

---

## 📈 Final Output

Generated file:
Contains:

- 46 rows
- 20 required columns
- Fraud validation result (`is_business_logic_valid`)

---

## 📁 Project Structure
---

## ▶️ How to Run Without Docker

From the project directory:

```bash
python script.py
output will be generated in:
output/referral_report.csv
🐳 How to Run With Docker
Step 1 — Build Docker Image
docker build -t referral-pipeline .
Step 2 — Run Container
PowerShell:
docker run -v ${PWD}/output:/app/output referral-pipeline
📘 Documentation Included
Data Dictionary: data_dictionary.xlsx
Data Profiling Report: data_profiling_report.csv
🛠 Technologies Used
Python 3.10
Pandas
Docker
WSL2 (Windows Subsystem for Linux)
✅ Key Data Engineering Practices Demonstrated
Controlled joins
Handling one-to-many relationships
Log table aggregation
Referential integrity enforcement
Business rule validation
Fraud detection logic implementation
Docker containerization
Clean and maintainable project structure
🏁 Conclusion
This solution demonstrates a complete data engineering workflow including:
Data profiling
Data cleaning
Controlled joins
Fraud detection
Report generation
Containerized deployment
The final dataset correctly produces 46 validated referral records as required.
