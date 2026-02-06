# 🛡️ FinGuard 22M: End-to-End Scalable Data Warehouse
> *"Turning 22.2 Million Raw Transactions into Actionable Financial Guard-Rails"*

## 🏗️ 1. Project Design & Architecture
![Project Design](https://github.com/YoussefHamedddd/Finance-Data-Warehouse/blob/main/docs/Project%20Design.png?raw=true)

**Overview:**
The system is built on a scalable pipeline that ensures data integrity from ingestion to visualization. It follows the **Medallion Architecture** (Bronze, Silver, Gold) to transform 22.2M raw records into a clean, analytical format.

**The Engineering Journey:**
* **The Pandas Struggle:** Initially, I used Pandas for extraction, but it crashed due to the massive 22M+ row volume.
* **JSON Optimization:** I implemented flattening strategies for massive, nested JSON files to prevent memory bottlenecks.
* **The DuckDB Solution:** I migrated to **DuckDB** to optimize the Load phase, achieving lightning-fast transfers and minimal memory consumption.

---

## ⚙️ 2. Data Pipeline & Orchestration (Airflow)
![Airflow DAG](https://github.com/YoussefHamedddd/Finance-Data-Warehouse/blob/main/docs/AirflowDags.png?raw=true)

**Orchestration Details:**
I utilized **Apache Airflow** within a **Docker** environment to automate the entire ETL process.
* **Extraction:** Multi-source ingestion from 3 CSVs and 2 JSONs.
* **Bronze Layer:** Raw data landing via DuckDB/Python.
* **Silver Layer:** Hardcore data cleansing and standardization using **dbt**.
* **Gold Layer:** Final transformation into a high-performance analytics layer.

---

## 📐 3. Data Modeling (Star Schema)
![Star Schema](https://github.com/YoussefHamedddd/Finance-Data-Warehouse/blob/main/docs/Star%20Schema.png?raw=true)

**Modeling Strategy:**
To ensure high-speed analytical queries, the warehouse is organized into a **Star Schema**.
* **Fact Table:** Centralized transaction records (22.2M rows).
* **Dimension Tables:** Dedicated tables for **Users** and **Cards**, allowing for efficient filtering and complex joins without compromising performance.

---

## 📊 4. Business Intelligence & Dashboards
![Overview Dashboard](https://github.com/YoussefHamedddd/Finance-Data-Warehouse/blob/main/Dashboard%20Photos/Overview.png?raw=true)

**Fraud-Centric Analytics (The Core):**
My primary focus was to unmask fraudulent patterns across the dataset using three pillars:
1.  **Risk Quantification:** Using the **0.1% Fraud Ratio** as a primary KPI for immediate health checks.
2.  **Geospatial Analysis:** Mapping transactions to identify high-risk geographic clusters.
3.  **Spending Correlation:** Analyzing the **$571.8M total spending** to spot suspicious spikes.

> **Key Finding:** Despite a "small" 0.1% fraud rate, at this scale, it represents **$1.4M+ in potential losses**, proving the necessity of this robust pipeline.

## 🧠 5. User Behavior & Fraud Insights
![User Behavior Chart](https://github.com/YoussefHamedddd/Finance-Data-Warehouse/blob/main/Dashboard%20Photos/User%20Behvior.png?raw=true)

**The Discovery:**
Beyond the engineering metrics, the data revealed a critical behavioral pattern:
* **Targeted Vulnerability:** Fraudulent activities are heavily concentrated among **lower-income brackets**. 
* **The Evidence:** My "Income vs. Spending" analysis shows dense clusters of fraud (red points) in lower-income segments.
* **Proactive Protection:** By identifying these high-risk user clusters, financial institutions can implement targeted security measures before theft occurs.

---
---

## 💡 6. Final Takeaway
Engineering is not just about making things work; it's about **optimization and strategic decision-making**. Choosing DuckDB over Pandas wasn't just a technical fix—it was a business-driven decision to reduce costs and processing time.

---

## 🛠️ Tech Stack
* **Languages:** Python, SQL
* **Orchestration:** Apache Airflow
* **Transformation:** dbt
* **Database:** PostgreSQL & DuckDB
* **Containerization:** Docker
* **Visualization:** Power BI
* **Environment:** VSCode & WSL

---

## 🤝 Let's Connect!
I am **open to discussing** this project, architectural choices, or Data Engineering opportunities. 

**GitHub Repository:** [Finance-Data-Warehouse](https://github.com/YoussefHamedddd/Finance-Data-Warehouse.git)
