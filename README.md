# Apache Airflow Learning Projects 🚀  

This repository contains my hands-on practice projects and exercises from the course [Learning Apache Airflow](https://www.linkedin.com/learning/learning-apache-airflow).  

The goal of this repo is to document my journey in mastering **workflow orchestration** with Apache Airflow, and to showcase practical implementations of data pipelines that demonstrate my skills as a future-ready **Data Engineer**.  

---

## 📌 What’s Inside  

### 1️⃣ Airflow Setup & Architecture  
- Local Airflow setup with **Docker** & basic DAGs  
- Understanding DAG structure (`tasks`, `operators`, `scheduling`)

### 2️⃣ Upstream & Downstream Dependencies  
- Managing task dependencies (`set_upstream`, `set_downstream`)  
- Parallel vs. sequential workflows  

### 3️⃣ SQL Pipelines  
- Example pipelines connecting to a relational database  
- ETL process using SQL operators  

### 4️⃣ Python Pipelines  
- Building pipelines with `PythonOperator`  
- Data transformations using custom Python functions  

### 5️⃣ XCom Usage  
- Passing data between tasks with **XCom** (`push` & `pull`)  
- Creating interdependent workflows  

### 6️⃣ Scheduling & Backfill  
- Configuring **cron-based schedules**  
- Running historical jobs with `backfill`  



---

## 🛠️ Tech Stack  

- **Apache Airflow** (DAGs, Operators, XCom, Scheduler)  
- **Python** (data transformations, custom tasks)  
- **SQL** (ETL processes & pipelines)  
- **Docker** (environment setup)  

---

## 🎯 Why This Project  

I completed this course and repo as part of my journey to strengthen my **Data Engineering** skills.  
With Airflow I can:  

- Orchestrate reliable & scalable data pipelines  
- Automate ETL / ELT workflows  
- Build strong foundations for **Machine Learning** & **BI projects**  

---

## 💻 Example Commands  

```bash
# List available DAGs
airflow dags list  

# Trigger a DAG manually
airflow dags trigger my_dag  

# Test a specific task
airflow tasks test my_dag my_task 2023-09-15  

# Backfill a DAG for historical runs
airflow dags backfill -s 2023-01-01 -e 2023-01-31 my_dag  
