# DSC202 - Fitness Tracker Data Management & Visualization

## 📌 Project Overview

This project focuses on managing and visualizing fitness tracker data using **PostgreSQL (AWS RDS), Neo4j, and a Streamlit web app**. The system processes user activity data, computes **MET values**, detects **anomalies**, and provides **insights through interactive visualizations**.

## 🔗 Links

- **📽️ Presentation:** [Click Here](https://ucsdcloud-my.sharepoint.com/:v:/g/personal/rkachroo_ucsd_edu/Ee3Hilzws6lKuGR4vkkJxpcBtznyUDpNQf3SZKZMMIKJpg?e=sT5J15&nav=eyJyZWZlcnJhbEluZm8iOnsicmVmZXJyYWxBcHAiOiJTdHJlYW1XZWJBcHAiLCJyZWZlcnJhbFZpZXciOiJTaGFyZURpYWxvZy1MaW5rIiwicmVmZXJyYWxBcHBQbGF0Zm9ybSI6IldlYiIsInJlZmVycmFsTW9kZSI6InZpZXcifX0%3D)
- **📂 GitHub Repository:** [DSC202](https://github.com/Chiragga24/DSC202)
- **📊 Live Streamlit App:** [Test the App](https://dsc202-project.streamlit.app/)

## 🛠️ Tech Stack

- **Database:** PostgreSQL (AWS RDS), Neo4j
- **Backend Processing:** AWS Lambda, Python
- **Frontend:** Streamlit
- **Deployment:** AWS & Streamlit Cloud

## 📋 Features

✔️ **Real-time and historical fitness data visualization**  
✔️ **Daily MET value computation** for different time zones  
✔️ **Activity anomaly detection**  
✔️ **User demographics management**  
✔️ **Graph-based insights using Neo4j**

## 📈 Database Architecture

### **PostgreSQL (AWS RDS)**

- Stores raw minute-level fitness data.
- Aggregates hourly and daily stats.
- Uses triggers to call AWS Lambda functions for processing.
- Also stores MET and metric-anomaly data

### Neo4j (Graph Database)

- Stores user relationships with MET scores and health metrics.
- Tracks daily summaries, anomalies, and activity clusters

## ⚡ AWS Lambda Functions

The project utilizes four **AWS Lambda** functions to automate processing:

1. **`clustering_neo4j.py`**

   - Runs every 48 hours.
   - Clusters Neo4j user nodes based on health metrics and demographics.

2. **`daily_update.py`**

   - Runs every 24 hours.
   - Aggregates data from hourly to daily level for each user.

3. **`hourly_update.py`**

   - Runs every hour.
   - Aggregates data from minute to hourly level for each user.

4. **`neo4j-sync-update.py`**

   - Triggered when new data is inserted in PostgreSQL (`user_demo` or `daily_demo`).
   - Fetches daily metrics and 7-day average MET values
   - If a user does not exist in Neo4j, it creates a User Node and establishes relationships for daily metrics and MET data.

## Dataset

The project uses Fitbit activity data sourced from Kaggle. You can access the dataset here:

[Fitbit Dataset on Kaggle](https://www.kaggle.com/datasets/arashnic/fitbit)

Note: Due to storage constraints, the full dataset is **not uploaded** to this repository. However, sample data is included, and the schema is described in the project report.

## 📊 Data Schema & Sample Data

The **database schema** and structure are documented in our **project report** (included in the repository).  
Since the dataset is **large**, we did not upload the full data, but **sample data is provided in the repository** to allow easy testing.

## 🚀 How to Run Locally

### **1️⃣ Clone the Repository**

```bash
git clone https://github.com/Chiragga24/DSC202.git
cd DSC202
```

### **2️⃣ Install Dependencies**

Ensure you have Python 3.8+ installed, then run:

```bash
pip install -r requirements.txt
```

### **3️⃣ Run the Streamlit App**

```bash
streamlit run app.py

```

## 📊 How to Test

Use the following **User IDs** to explore data in the app:

- 99990003
- 5553957443
- 6962181067

## 👥 Team Members

- Chirag Agarwal - A69034328

- Raghav Kachroo - A69035155

- Hemanth Bodala - A69037783
