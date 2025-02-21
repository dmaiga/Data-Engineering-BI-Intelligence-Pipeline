import os
import pandas as pd
import psycopg2
from dotenv import load_dotenv

# Charger les variables depuis .env
load_dotenv()

# Configuration PostgreSQL
PG_HOST = os.getenv("PG_HOST")
PG_PORT = int(os.getenv("PG_PORT"))  # Convertir en entier
PG_DATABASE = os.getenv("PG_DATABASE")
PG_USER = os.getenv("PG_USER")
PG_PASSWORD = os.getenv("PG_PASSWORD")

# Fichiers CSV
CSV_FILES = {
    "dm_ventes_produit_table.csv": "dm_ventes_produit_table",
    "dm_tendances_locales_table.csv": "dm_tendances_locales_table"
}

# Creation des tables
CREATE_TABLE_QUERIES = {
    "dm_ventes_produit_table": """
        CREATE TABLE IF NOT EXISTS dm_ventes_produit_table (
            produit_id VARCHAR(255),
            produit_nom VARCHAR(255),
            categorie VARCHAR(255),
            mois INTEGER,
            annee INTEGER,
            quantite_totale INTEGER,
            revenu_total FLOAT,
            profit_total FLOAT
        );
    """,
    "dm_tendances_locales_table": """
        CREATE TABLE IF NOT EXISTS dm_tendances_locales_table (
            city VARCHAR(255),
            mois INTEGER,
            annee INTEGER,
            productid_phare VARCHAR(255),
            total_ventes FLOAT,
            quantite_vendue INTEGER
        );
    """
}

def load_csv_to_postgresql(csv_file, table_name):
    try:
        # Connexion à PostgreSQL
        connection = psycopg2.connect(
            host=PG_HOST,
            port=PG_PORT,
            database=PG_DATABASE,
            user=PG_USER,
            password=PG_PASSWORD,
        )
        cursor = connection.cursor()

        # Créer la table si elle n'existe pas
        cursor.execute(CREATE_TABLE_QUERIES[table_name])
        connection.commit()

        # Charger les données
        df = pd.read_csv(csv_file)

        # Transformer les valeurs négatives si nécessaire
        if table_name == "dm_ventes_produit_table":
            df["profit_total"] = df["profit_total"].abs()  # Valeur absolue
        elif table_name == "dm_tendances_locales_table":
            df["total_ventes"] = df["total_ventes"].abs()  # Valeur absolue

        # Charger les données
        for _, row in df.iterrows():
            placeholders = ", ".join(["%s"] * len(row))
            insert_query = f"INSERT INTO {table_name} VALUES ({placeholders})"
            cursor.execute(insert_query, tuple(row))
        connection.commit()
        print(f"Données du fichier {csv_file} chargées dans la table {table_name}.\n")
    except Exception as e:
        print(f"Erreur : {e}")
    finally:
        if connection:
            cursor.close()
            connection.close()

if __name__ == "__main__":
    for csv_file, table_name in CSV_FILES.items():
        load_csv_to_postgresql(csv_file, table_name)