from google.cloud import bigquery
import pandas as pd

# Configuration BigQuery
PROJECT_ID = "sid-ecommerce-450600"
DATASET = "ecommerce_raw_data"

# Tables à extraire
TABLES = {
    "dm_ventes_produit_table": "dm_ventes_produit_table.csv",
    "dm_tendances_locales_table": "dm_tendances_locales_table.csv"
}

def fetch_table_to_csv(table_name, output_file):
    client = bigquery.Client(project=PROJECT_ID)
    query = f"SELECT * FROM `{PROJECT_ID}.{DATASET}.{table_name}`"
    query_job = client.query(query)

    # Convertir les résultats en DataFrame
    df = query_job.result().to_dataframe()
    print(f"Table {table_name} : {len(df)} lignes extraites.")

    # Sauvegarder en CSV
    df.to_csv(output_file, index=False)
    print(f"Table {table_name} enregistrée sous {output_file}.\n")

if __name__ == "__main__":
    for table, output in TABLES.items():
        fetch_table_to_csv(table, output)
