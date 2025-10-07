from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import psycopg2

def extract_html_case_data():
    print("extracting html case data...")
    # html_data = requests.get(url).text
    # return html_data

def transform_data_for_analysis():
    print("transforming data for analysis...")
    # structured_data = parse_html(html_data)
    # return structured_data

def parse_json_data():
    print("parsing json data...")
    # parsed_data = json.loads(transformed_data)
    # return parsed_data

def lint_json_data():
    print("linting json data...")
    # check required fields, missing values, duplicates
    # validate(parsed_data)

def load_data_to_postgres_db():
    print("loading data into postgres...")
    conn = psycopg2.connect(
        dbname="justicetech_db",
        user="airflow_user",
        password="justiceTech123",
        host="localhost",
        port="5432"
    )
    cur = conn.cursor()
    # exectute insert statements into tables
    # cur.execute("INSERT INTO my_table (col1) VALUES ('example');")
    conn.commit()
    cur.close()
    conn.close()
    print("data loaded successfully!")

def further_transform_data():
    print("further transforming data...")
    # call funcs w/normalize fields, create summary stats

with DAG(
    dag_id="example_dag",
    start_date=datetime(2025, 10, 7),
    schedule_interval="@daily",
    catchup=False,
) as dag:

    task1 = PythonOperator(
        task_id="extract_html_case_data",
        python_callable=lambda: print("Extracting HTML case data...")
    )

    task2 = PythonOperator(
        task_id="transform_data_for_analysis",
        python_callable=lambda: print("Transforming data for analysis...")
    )

    task3 = PythonOperator(
        task_id="parse_json_data",
        python_callable=lambda: print("Parsing JSON data...")
    )

    task4 = PythonOperator(
        task_id="lint_json_data",
        python_callable=lambda: print("Linting JSON data...")
    )
    
    task5 = PythonOperator(
        task_id="transform_data",
        python_callable=lambda: print("Transforming data...")
    )

    task6 = PythonOperator(
        task_id="load_data_to_postgres_db",
        python_callable=lambda: print("Loading data to Postgres...")
    )

    # scheduled tasks order
    task1 >> task2 >> task3 >> task4 >> task5 >> task6
