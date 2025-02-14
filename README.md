# Data Engineering & Business Intelligence Pipeline

## Project Overview

This project demonstrates an end-to-end Data Engineering and Business Intelligence (BI) workflow. It begins with downloading a dataset, loading it into Google BigQuery, transforming the data to create facts and dimensions tables, building data marts, and concludes with reporting and visualization using Power BI. The project emphasizes the integration of modern data tools to deliver actionable insights.

---

## Workflow

### 1. **Dataset Download**

- A dataset was downloaded from an online source to serve as the foundation for this project.

### 2. **Data Storage: Google BigQuery**

- The dataset was uploaded into Google BigQuery for efficient storage and query processing at scale.

### 3. **Data Transformation: dbt (Data Build Tool)**

- Leveraged dbt to perform data transformations:
  - Cleaned and prepared raw data.
  - Created **fact tables** and **dimension tables** to support analytics.
  - Designed and implemented two **data marts** for specific reporting needs.

### 4. **Data Extraction and Loading**

- Extracted data marts from BigQuery in CSV format using a Python script.
- Loaded the extracted data into a PostgreSQL database using another Python script.

### 5. **Reporting & Visualization: Power BI**

- Built an interactive dashboard using Power BI.
- Connected Power BI directly to BigQuery for real-time reporting capabilities.

---

## Tools & Technologies Used

### Data Storage & Processing

- **Google BigQuery**: For large-scale data storage and query processing.
- **PostgreSQL**: For local database storage after extraction.

### Data Transformation

- **dbt (Data Build Tool)**: For modular and scalable data transformations.

### Scripting & Automation

- **Python**:
  - Data extraction and CSV generation from BigQuery.
  - Data loading into PostgreSQL.
  - Libraries used: `pandas`, `sqlalchemy`, `google-cloud-bigquery`.

### Reporting & Visualization

- **Power BI**: For creating dynamic dashboards and visualizations.

---

## Key Deliverables

1. **Fact and Dimension Tables**: Structured and optimized for analytics.
2. **Data Marts**: Designed for specific business use cases.
3. **Power BI Dashboard**: Providing insights and enabling data-driven decisions.

---

## How to Run the Project

### Prerequisites

- A Google Cloud account with BigQuery access.
- PostgreSQL installed locally or on a server.
- Python (with the necessary libraries installed). See `requirements.txt` for dependencies.

### Steps

1. Clone this repository to your local machine.
2. Download the dataset and upload it to BigQuery.
3. Use dbt to execute data transformations and create data marts.
4. Run the Python scripts in the `scripts/` folder:
   - `extract_data.py`: Extracts data marts from BigQuery as CSV files.
   - `load_to_postgres.py`: Loads the extracted data into PostgreSQL.
5. Open Power BI and connect to BigQuery to create the dashboard.

---

## Future Improvements

- Automate the entire pipeline using Apache Airflow.
- Add unit tests for Python scripts to improve reliability.
- Explore advanced visualizations and insights using AI/ML tools.

---

## Author

MAIGA Mahamane Daouda\
Feel free to reach out for any questions or collaborations!

