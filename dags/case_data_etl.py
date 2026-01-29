from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from dotenv import load_dotenv
import psycopg2
from bs4 import BeautifulSoup
import os
import sys
import re

# add project root to Python path
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
    """Extract new addresses AND geocode existing ones using CURA service"""
    import time
        
    conn = psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT
    )
    cur = conn.cursor()
    
    start_time = time.time()
    max_runtime = 6 * 60  # 6 minutes absolute max runtime
    
    # extract new addresses from unprocessed cases
    cur.execute("""
        SELECT r.case_number, r.raw_html 
        FROM raw_cases r
        LEFT JOIN address a ON r.case_number = a.case_number
        WHERE a.case_number IS NULL 
        AND r.raw_html IS NOT NULL
        LIMIT 20
    """)
    
    unprocessed_cases = cur.fetchall()    
    total_extracted = 0
    
    for case_number, raw_html in unprocessed_cases:
        if time.time() - start_time > 60:  
            print("time limit reached for extraction")
            break
            
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
                        'extracted'
                    ))
                    total_extracted += 1
                except Exception as e:
                    print(f"Error inserting address for case {case_number}: {e}")
            
        except Exception as e:
            print(f"Error processing addresses for case {case_number}: {e}")
    
    conn.commit()
    print(f"extracted {total_extracted} new addresses")
    
    # geocode existing addresses (mock + extracted) using CURA    
    # initialize CURA geocoding service
    try:
        sys.path.append('/Users/dishapatel/airflow_scheduling')
        from utils.geolocation import create_geolocation_service
        geo_service = create_geolocation_service()
    except Exception as e:
        print(f"failed to initialize CURA geocoding service: {e}")
        cur.close()
        conn.close()
        return
    
    # fixed batch size for reliable DAG execution - process 8 addresses per DAG run
    batch_size = 8  
    
    cur.execute("""
        SELECT address_id, case_number, address_line1, city, state, postal_code, geocode_status
        FROM address
        WHERE geocode_status IN ('mock', 'extracted')
        AND address_line1 IS NOT NULL
        AND address_line1 != ''
        AND (latitude = 0 OR latitude IS NULL)
        ORDER BY 
            CASE geocode_status 
                WHEN 'mock' THEN 1 
                WHEN 'extracted' THEN 2 
            END,
            address_id ASC
        LIMIT %s
    """, (batch_size,))
    
    addresses_to_geocode = cur.fetchall()
    geocoded_count = 0
    
    if not addresses_to_geocode:
        print("NO mock or extracted addresses found to geocode!")
        cur.close()
        conn.close()
        return
    
    for i, (address_id, case_number, address_line1, city, state, postal_code, status) in enumerate(addresses_to_geocode):
        if time.time() - start_time > max_runtime:
            print(f"runtime limit reached, stopping at address {i}")
            break
            
        print(f"\n  Processing address {i+1}/{len(addresses_to_geocode)}:")
        print(f"    ID: {address_id} | Status: {status}")
        print(f"    Address: {address_line1}, {city}, {state} {postal_code}")
        
        try:
            # geocode using CURA
            address_start = time.time()
            result = geo_service.geocode_address({
                'address_line1': address_line1,
                'city': city if city != 'Unknown' else '',
                'state': state or 'OH',
                'postal_code': postal_code or ''
            })
            
            geocode_time = time.time() - address_start
            print(f"      CURA response time: {geocode_time:.2f} seconds")
            
            # skip addresses that take too long (timeout protection)
            if geocode_time > 20:
                print(f"address {address_id} took {geocode_time:.1f}s, marking as timeout")
                cur.execute("""
                    UPDATE address SET geocode_status = 'timeout' WHERE address_id = %s
                """, (address_id,))
                continue
            
            if result['status'] == 'success':
                # update database with real coordinates
                cur.execute("""
                    UPDATE address 
                    SET latitude = %s, longitude = %s, geocode_status = %s, geocoded_at = %s
                    WHERE address_id = %s
                """, (
                    result['latitude'],
                    result['longitude'],
                    'success',
                    datetime.now(),
                    address_id
                ))
                geocoded_count += 1
                print(f"      SUCCESS: {result['latitude']:.6f}, {result['longitude']:.6f}")
                
                # update city if it was Unknown
                if city == 'Unknown' and result.get('address_details', {}).get('City'):
                    geocoded_city = result['address_details']['City']
                    cur.execute("""
                        UPDATE address SET city = %s WHERE address_id = %s
                    """, (geocoded_city, address_id))
                else:
                    # update status to indicate failure
                    cur.execute("""
                        UPDATE address SET geocode_status = %s WHERE address_id = %s
                    """, (result['status'], address_id))
            
            # save progress frequently (every 2 addresses)
            if (i + 1) % 2 == 0:
                conn.commit()
                print(f"      Progress saved: {i + 1}/{len(addresses_to_geocode)} processed")
            
            # small delay for CURA service
            time.sleep(0.5)
                
        except Exception as e:
            print(f"       ERROR: {e}")
            cur.execute("""
                UPDATE address SET geocode_status = 'error' WHERE address_id = %s
            """, (address_id,))
    
    conn.commit()
    
    cur.execute("SELECT geocode_status, COUNT(*) FROM address GROUP BY geocode_status ORDER BY COUNT(*) DESC")
    status_counts = cur.fetchall()
    
    cur.execute("SELECT COUNT(*) FROM address WHERE geocode_status IN ('mock', 'extracted') AND (latitude = 0 OR latitude IS NULL)")
    remaining = cur.fetchone()[0]
    
    runtime = time.time() - start_time
    print(f"\n. TASK COMPLETE:")
    print(f"    New addresses extracted: {total_extracted}")
    print(f"    Addresses geocoded: {geocoded_count}")
    print(f"    Remaining to process: {remaining}")
    print(f"    Total runtime: {runtime:.1f} seconds")
    print(f"    Current status counts:")
    for status, count in status_counts:
        print(f"     {status}: {count}")
    
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
    schedule_interval="*/15 * * * *",  # every 15 minutes for steady progress
    catchup=False,
    description="Process HTML case files and geocode addresses with timeout protection",
    tags=["etl", "cases", "geocoding", "extraction"],
    max_active_runs=1,  # prevent overlapping runs
    dagrun_timeout=timedelta(minutes=10),  # kill entire DAG task run after 10 minutes
    default_args={
        'execution_timeout': timedelta(minutes=8),  # 8-minute timeout per task
        'retries': 0,  # no retries to prevent overlap
        'retry_delay': timedelta(minutes=5),
    }
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
    
    