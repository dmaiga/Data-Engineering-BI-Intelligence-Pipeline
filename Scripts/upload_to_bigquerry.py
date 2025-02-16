from google.cloud import bigquery
from google.cloud.exceptions import NotFound
import os

# Configurations
PROJECT_ID = "sid-ecommerce-450600"  
DATASET_ID = "ecommerce_raw_data"   
TABLE_ID = "raw_data"               
CSV_FILE_PATH = "data/ecommerce_data.csv"  # Path to the CSV file

def upload_csv_to_bigquery(project_id, dataset_id, table_id, file_path):
    """
    Uploads a CSV file to a BigQuery table.
    """
    client = bigquery.Client(project=project_id)
    table_ref = client.dataset(dataset_id).table(table_id)

    # Check if the table exists
    try:
        client.get_table(table_ref)
        print(f"Table {table_id} already exists in dataset {dataset_id}.")
    except NotFound:
        print(f"Table {table_id} does not exist. Creating the table...")
        schema = [
            bigquery.SchemaField("Row_ID", "INTEGER"),
            bigquery.SchemaField("Order_ID", "STRING"),
            bigquery.SchemaField("Year", "INTEGER"),
            bigquery.SchemaField("Order_Date", "DATE"),
            bigquery.SchemaField("Ship_Date", "DATE"),
            bigquery.SchemaField("Ship_Mode", "STRING"),
            bigquery.SchemaField("Customer_ID", "STRING"),
            bigquery.SchemaField("Customer_Name", "STRING"),
            bigquery.SchemaField("Segment", "STRING"),
            bigquery.SchemaField("Country", "STRING"),
            bigquery.SchemaField("City", "STRING"),
            bigquery.SchemaField("State", "STRING"),
            bigquery.SchemaField("Postal_Code", "INTEGER"),
            bigquery.SchemaField("Region", "STRING"),
            bigquery.SchemaField("Product_ID", "STRING"),
            bigquery.SchemaField("Category", "STRING"),
            bigquery.SchemaField("Sub_Category", "STRING"),
            bigquery.SchemaField("Product_Name", "STRING"),
            bigquery.SchemaField("Sales", "FLOAT"),
            bigquery.SchemaField("Quantity", "INTEGER"),
            bigquery.SchemaField("Discount", "FLOAT"),
            bigquery.SchemaField("Profit", "FLOAT"),
        ]
        table = bigquery.Table(table_ref, schema=schema)
        client.create_table(table)
        print(f"Table {table_id} created successfully.")

    # Load the CSV file
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        autodetect=False,  # 
    )
    with open(file_path, "rb") as csv_file:
        load_job = client.load_table_from_file(csv_file, table_ref, job_config=job_config)
    
    load_job.result()  #
    print(f"CSV file {file_path} uploaded successfully to {dataset_id}.{table_id}.")

if __name__ == "__main__":
    if not os.path.exists(CSV_FILE_PATH):
        print(f"Error: CSV file not found at {CSV_FILE_PATH}.")
    else:
        upload_csv_to_bigquery(PROJECT_ID, DATASET_ID, TABLE_ID, CSV_FILE_PATH)
