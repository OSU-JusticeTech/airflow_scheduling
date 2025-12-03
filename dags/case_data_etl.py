from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from dotenv import load_dotenv
import psycopg2
from bs4 import BeautifulSoup
import os

# change this to your local path where HTML files are stored -- and consider moving it to .env variables
CASES_FILE_DIRECTORY = os.path.expanduser("~/Desktop/justicetech/cases")
BATCH_SIZE = 1000
# under public schema now 
TABLE_NAME = "raw_case_data"

load_dotenv()

DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_USER = os.getenv("DB_USER")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")


def batch_html_to_postgres():
    # works only when given provileges such as superuser --FIX: still updating to user airflow_user to work
    conn = psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT
    )
    cur = conn.cursor()

    # find all HTML files in the directory
    html_files = [f for f in os.listdir(CASES_FILE_DIRECTORY) if f.endswith(".html")]
    total_files = len(html_files)
    print(f"Found {total_files} HTML files.")

    # process files in batches
    for i in range(0, total_files, BATCH_SIZE):
        batch = html_files[i:i+BATCH_SIZE]
        records = []

        for file_name in batch:
            file_path = os.path.join(CASES_FILE_DIRECTORY, file_name)
            try:
                with open(file_path, "r", encoding="utf-8") as f:
                    content = f.read()
                    soup = BeautifulSoup(content, "html.parser")
                    text = soup.get_text(separator=" ", strip=True)
                    records.append((file_name, text))
            except Exception as e:
                print(f"Error processing {file_name}: {e}")

        # bulk insert into postgres
        cur.executemany(f"""
            INSERT INTO {TABLE_NAME} (file_name, extracted_text)
            VALUES (%s, %s)
        """, records)
        conn.commit()
        print(f"Inserted batch {i // BATCH_SIZE + 1} ({len(records)} records)")

    cur.close()
    conn.close()
    print("All batches processed successfully!")

with DAG(
    dag_id="batch_cases_to_postgres",
    start_date=datetime(2025, 10, 22),
    schedule_interval="@daily",
    catchup=False,
) as dag:

    task_batch_stream = PythonOperator(
        task_id="batch_html_to_postgres",
        python_callable=batch_html_to_postgres
    )
    
    
