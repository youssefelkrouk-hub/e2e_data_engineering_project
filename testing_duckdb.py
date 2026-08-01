import duckdb

# Chemin vers ton fichier DuckDB
DUCKDB_PATH = "/opt/airflow/db/ecommerce_data.duckdb"

conn = duckdb.connect(DUCKDB_PATH, read_only=True)

# Lister tous les schémas
print("=== Schémas ===")
print(conn.execute("SELECT schema_name FROM information_schema.schemata").fetchall())

# Lister toutes les tables
print("\n=== Tables ===")
print(conn.execute("SHOW ALL TABLES").fetchall())

# Vérifier si raw_layer.sales existe et afficher un échantillon
try:
    count = conn.execute("SELECT COUNT(*) FROM raw_layer.sales").fetchone()
    print(f"\n=== Nombre de lignes dans raw_layer.sales : {count[0]} ===")

    print("\n=== Échantillon (10 lignes) ===")
    df = conn.execute("SELECT * FROM raw_layer.sales LIMIT 10").fetchdf()
    print(df)

except Exception as e:
    print(f"\nErreur : {e}")

conn.close()