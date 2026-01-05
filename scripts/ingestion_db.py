import os
import psycopg2
import pandas as pd
import logging
import time

# --------------------------------------------------
# LOGGING
# --------------------------------------------------
os.makedirs("logs", exist_ok=True)

logging.basicConfig(
    filename="logs/ingestion_db.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filemode="a"
)

# --------------------------------------------------
# POSTGRES CONNECTION
# --------------------------------------------------
conn = psycopg2.connect(
    host="localhost",
    port=5432,
    database="inventory",
    user="postgres",
    password="root"
)
conn.autocommit = True
cursor = conn.cursor()

# --------------------------------------------------
# HELPER: MAP PANDAS → POSTGRES TYPES
# --------------------------------------------------
def pandas_to_postgres(dtype):
    if pd.api.types.is_integer_dtype(dtype):
        return "BIGINT"
    elif pd.api.types.is_float_dtype(dtype):
        return "DOUBLE PRECISION"
    elif pd.api.types.is_bool_dtype(dtype):
        return "BOOLEAN"
    elif pd.api.types.is_datetime64_any_dtype(dtype):
        return "TIMESTAMP"
    else:
        return "TEXT"

# --------------------------------------------------
# CREATE TABLE FROM CSV
# --------------------------------------------------
def create_table_from_csv(csv_path, table_name):
    sample = pd.read_csv(csv_path, nrows=1000)

    columns = []
    for col, dtype in sample.dtypes.items():
        pg_type = pandas_to_postgres(dtype)
        columns.append(f'"{col}" {pg_type}')

    ddl = f"""
    DROP TABLE IF EXISTS "{table_name}";
    CREATE TABLE "{table_name}" (
        {", ".join(columns)}
    );
    """

    cursor.execute(ddl)
    logging.info(f"Table created: {table_name}")

# --------------------------------------------------
# COPY DATA INTO TABLE
# --------------------------------------------------
def copy_csv_to_table(csv_path, table_name):
    logging.info(f"Started COPY for {table_name}")
    start = time.time()

    create_table_from_csv(csv_path, table_name)

    with open(csv_path, 'r', encoding='utf-8') as f:
        cursor.copy_expert(
            sql=f'''
            COPY "{table_name}"
            FROM STDIN
            WITH (FORMAT CSV, HEADER TRUE)
            ''',
            file=f
        )

    total = round((time.time() - start) / 60, 2)
    logging.info(f"Completed COPY for {table_name} in {total} minutes")

# --------------------------------------------------
# LOAD ALL CSV FILES
# --------------------------------------------------
def load_all_csvs():
    logging.info("-------------- FAST INGESTION STARTED --------------")
    overall_start = time.time()

    for file in os.listdir("data"):
        if file.lower().endswith(".csv"):
            table_name = file.replace(".csv", "")
            csv_path = os.path.join("data", file)

            logging.info(f"Processing {file}")
            copy_csv_to_table(csv_path, table_name)

    overall = round((time.time() - overall_start) / 60, 2)
    logging.info("-------------- INGESTION COMPLETE --------------")
    logging.info(f"Total Time Taken: {overall} minutes")

# --------------------------------------------------
# MAIN
# --------------------------------------------------
if __name__ == "__main__":
    load_all_csvs()
