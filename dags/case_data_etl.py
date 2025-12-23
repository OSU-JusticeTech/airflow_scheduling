from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from dotenv import load_dotenv
import psycopg2
from bs4 import BeautifulSoup
import os
import sys
import re

# Add the project root to Python path
sys.path.append('/Users/dishapatel/airflow_scheduling')

try:
    from utils.parsers import extract_parties_from_text, extract_attorneys_from_text
except ImportError:
    print("Warning: Could not import parsers - address extraction will be disabled")

# change this to your local path where HTML files are stored -- and consider moving it to .env variables
CASES_FILE_DIRECTORY = os.path.expanduser("~/JusticeTech/cases")
BATCH_SIZE = 1000
# under public schema now 
TABLE_NAME = "raw_cases"

load_dotenv()

DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_USER = os.getenv("DB_USER")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")


def batch_html_to_postgres():
    # works only when given provileges such as superuser 
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
                    case_number = file_name.replace('.html', '')
                    records.append((case_number, content))
            except Exception as e:
                print(f"Error processing {file_name}: {e}")

        # bulk insert into postgres - match existing table structure
        cur.executemany(f"""
            INSERT INTO {TABLE_NAME} (case_number, raw_html)
            VALUES (%s, %s)
            ON CONFLICT (case_number) DO NOTHING
        """, records)
        conn.commit()
        print(f"Inserted batch {i // BATCH_SIZE + 1} ({len(records)} records)")

    cur.close()
    conn.close()
    print("All batches processed successfully!")


def extract_and_geocode_addresses():
    print("=== ADDRESS EXTRACTION AND GEOCODING TASK STARTING ===")
    
    conn = psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT
    )
    cur = conn.cursor()
    
    # get cases that don't have addresses extracted yet
    cur.execute("""
        SELECT r.case_number, r.raw_html 
        FROM raw_cases r
        LEFT JOIN address a ON r.case_number = a.case_number
        WHERE a.case_number IS NULL 
        AND r.raw_html IS NOT NULL
        LIMIT 100
    """)
    
    unprocessed_cases = cur.fetchall()    
    total_addresses = 0
    
    for case_number, raw_html in unprocessed_cases:
        try:
            addresses = extract_addresses_simple(raw_html, case_number)
            
            for addr in addresses:
                try:
                    cur.execute("""
                        INSERT INTO address (case_number, address_type, address_line1, city, state, 
                                           postal_code, geocode_status)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """, (
                        case_number,
                        addr.get('address_type', 'unknown'),
                        addr.get('address_line1', ''),
                        addr.get('city', ''),
                        addr.get('state', ''),
                        addr.get('postal_code', ''),
                        'extracted'  # Status indicates address was extracted but not yet geocoded
                    ))
                    total_addresses += 1
                except Exception as e:
                    print(f"Error inserting address for case {case_number}: {e}")
            
        except Exception as e:
            print(f"Error processing addresses for case {case_number}: {e}")
    
    conn.commit()
    cur.close()
    conn.close()
    

def extract_addresses_simple(html_content, case_number):
    addresses = []
    
    address_patterns = [
        r'(\d+\s+[A-Za-z\s]+(?:Street|St|Avenue|Ave|Road|Rd|Drive|Dr|Lane|Ln|Boulevard|Blvd))',
        r'(\d+\s+[A-Za-z\s]+\s+(?:OH|Ohio)\s+\d{5})'
    ]
    
    city_state_pattern = r'([A-Za-z\s]+),?\s*(OH|Ohio)\s*(\d{5})?'
    
    for i, pattern in enumerate(address_patterns):
        matches = re.findall(pattern, html_content, re.IGNORECASE)
        for match in matches[:2]: 
            city_match = re.search(city_state_pattern, html_content[max(0, html_content.find(match)-100):html_content.find(match)+100])
            
            addresses.append({
                'case_number': case_number,
                'address_type': f'extracted_{i}',
                'address_line1': match if isinstance(match, str) else match[0],
                'city': city_match.group(1).strip() if city_match else 'Unknown',
                'state': 'OH',
                'postal_code': city_match.group(3) if city_match and city_match.group(3) else ''
            })
    
    return addresses[:5] 


with DAG(
    dag_id="batch_cases_to_postgres",
    start_date=datetime(2025, 10, 22),
    schedule_interval="@daily",
    catchup=False,
    description="Process HTML case files and extract/geocode addresses",
    tags=["etl", "cases", "geocoding"],
) as dag:

    task_batch_html = PythonOperator(
        task_id="batch_html_to_postgres",
        python_callable=batch_html_to_postgres
    )
    
    task_extract_addresses = PythonOperator(
        task_id="extract_and_geocode_addresses", 
        python_callable=extract_and_geocode_addresses
    )

    # set task dependencies
    task_batch_html >> task_extract_addresses
    
    