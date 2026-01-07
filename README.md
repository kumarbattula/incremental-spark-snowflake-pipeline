# Incremental Transaction Processing with Spark & Snowflake

## 📌 Project Overview
This project demonstrates an incremental ETL pipeline using AWS Glue, Apache Spark, Amazon S3, and Snowflake.

The pipeline loads only new transaction files into Snowflake using an audit-based idempotent design.

## 🏗 Architecture
S3 → AWS Glue (Spark) → Snowflake  
Secrets managed via AWS Secrets Manager.

## 🔁 Incremental Logic
- Reads processed file names from Snowflake audit table
- Filters already processed files using Spark
- Loads only new (delta) data
- Writes audit metadata after successful load

## 🛠 Technologies Used
- AWS Glue (PySpark)
- Apache Spark
- Amazon S3
- Snowflake
- AWS Secrets Manager
- IAM

## 📂 Repository Structure
