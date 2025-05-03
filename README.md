# 🛒 Retail Metrics Pipeline

A scalable data pipeline built with **Apache Spark**, orchestrated using **Cloud Composer (Airflow)**, and deployed on **Google Cloud Platform** to calculate business-critical metrics like profit analysis, demand-supply ratios, and rewards program effectiveness.

---

## 🚀 Overview

This project automates the processing of retail sales data using PySpark and stores the aggregated metrics in Google Cloud Storage (GCS). The Spark job is triggered daily via an Apache Airflow DAG running on Google Cloud Composer.

---

## 🧱 Tech Stack

- **PySpark** (for data processing)
- **Apache Airflow (Cloud Composer)** (for orchestration)
- **Google Cloud Storage (GCS)** (for data I/O)
- **Google Cloud Dataproc** (for Spark job execution)
- **CSV Files** as input/output data format

---

## 📊 Metrics Computed

1. **Profit & Promo Indicator**
   - Calculates profit and checks if a product was under promotion.

2. **Demand & Supply Ratio**
   - Compares quantity sold vs quantity available for each product.

3. **Rewards Program Metrics**
   - Analyzes customer rewards: points earned vs redeemed.

---