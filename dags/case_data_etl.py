from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from dotenv import load_dotenv
import psycopg2
from bs4 import BeautifulSoup
import os
import sys
import re
from urllib.parse import quote_plus

# Add the project root to Python path so we can import our modules
sys.path.append('/Users/dishapatel/airflow_scheduling')

from utils.geolocation import create_geolocation_service
from utils.parsers import extract_parties_from_text, extract_attorneys_from_text

CASES_FILE_DIRECTORY = os.path.expanduser("~/JusticeTech/cases")
BATCH_SIZE = 1000
# under public schema now 
TABLE_NAME = "raw_cases"

# Load .env file from specific location only
env_path = "/Users/dishapatel/airflow_scheduling/.env"
if os.path.exists(env_path):
    load_dotenv(dotenv_path=env_path)
    print(f"Loaded environment from: {env_path}")
else:
    raise FileNotFoundError(f"Required .env file not found at: {env_path}")

DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_USER = os.getenv("DB_USER")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")

# Debug: Print loaded environment variables
print(f"Environment variables loaded:")
print(f"DB_NAME: {DB_NAME}")
print(f"DB_USER: {DB_USER}")
print(f"DB_HOST: {DB_HOST}")
print(f"DB_PORT: {DB_PORT}")
print(f"DB_PASSWORD: {'****' if DB_PASSWORD else 'None'}")
print(f"CASES_FILE_DIRECTORY: {CASES_FILE_DIRECTORY}")

def batch_html_to_postgres():
    """
    Process HTML files in batches and insert them into PostgreSQL database.
    Extracts case numbers from filenames and stores raw HTML content.
    """
    
    print("=== TASK STARTING ===")
    print(f"Environment variables at runtime:")
    print(f"DB_NAME: {DB_NAME}")
    print(f"DB_USER: {DB_USER}")  
    print(f"DB_HOST: {DB_HOST}")
    print(f"DB_PORT: {DB_PORT}")
    print(f"DB_PASSWORD: {'Set' if DB_PASSWORD else 'Not Set'}")
    
    # Check if all required variables are present
    if not all([DB_NAME, DB_USER, DB_HOST, DB_PORT, DB_PASSWORD]):
        missing = [var for var, val in [('DB_NAME', DB_NAME), ('DB_USER', DB_USER), 
                                      ('DB_HOST', DB_HOST), ('DB_PORT', DB_PORT), 
                                      ('DB_PASSWORD', DB_PASSWORD)] if not val]
        raise ValueError(f"Missing environment variables: {missing}")
    
    # Log connection info (without sensitive data)
    print(f"Connecting to database: {DB_NAME} at {DB_HOST}:{DB_PORT} as user {DB_USER}")
    
    try:
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT
        )
        cur = conn.cursor()
        print("✓ Database connection successful")
    except psycopg2.Error as e:
        print(f"✗ Error connecting to database: {e}")
        raise

    # Check if directory exists and find all HTML files
    if not os.path.exists(CASES_FILE_DIRECTORY):
        print(f"Error: Directory {CASES_FILE_DIRECTORY} does not exist")
        return
    
    html_files = [f for f in os.listdir(CASES_FILE_DIRECTORY) if f.endswith(".html")]
    total_files = len(html_files)
    print(f"Found {total_files} HTML files in {CASES_FILE_DIRECTORY}")

    # process files in batches
    for i in range(0, total_files, BATCH_SIZE):
        batch = html_files[i:i+BATCH_SIZE]
        records = []

        for file_name in batch:
            file_path = os.path.join(CASES_FILE_DIRECTORY, file_name)
            try:
                with open(file_path, "r", encoding="utf-8") as f:
                    content = f.read()
                    # Extract case number from filename (assuming format like "case_12345.html")
                    case_number = file_name.replace('.html', '').replace('case_', '')
                    records.append((case_number, content))
            except Exception as e:
                print(f"Error processing {file_name}: {e}")

        # bulk insert into postgres
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
    """
    Extract addresses from already processed raw_cases and geocode them using mock service.
    """
    
    print("=== ADDRESS EXTRACTION AND GEOCODING TASK STARTING ===")
    
    # Initialize geolocation service (using mock for now)
    print("✓ Initialized mock geolocation service")
    
    # Connect to database
    try:
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT
        )
        cur = conn.cursor()
        print("✓ Database connection successful")
    except psycopg2.Error as e:
        print(f"✗ Error connecting to database: {e}")
        raise
    
    # Process cases in batches
    for i in range(0, total_cases, BATCH_SIZE):
        batch = unprocessed_cases[i:i+BATCH_SIZE]
        address_records = []
        
        for case_number, raw_html in batch:
            try:
                # Extract addresses from HTML
                addresses = extract_addresses_from_html(raw_html, case_number)
                total_addresses_processed += len(addresses)
                
                # Geocode each address
                for addr in addresses:
                    try:
                        geocode_result = geo_service.geocode_address(addr)
                        
                        address_records.append((
                            case_number,  # Add case_number to the record
                            addr['address_type'],
                            addr['address_line1'],
                            addr['address_line2'],
                            addr['city'],
                            addr['state'],
                            addr['country'],
                            addr['postal_code'],
                            geocode_result['latitude'],
                            geocode_result['longitude'],
                            geocode_result['status']
                        ))
                        
                        if geocode_result['status'] in ['success', 'mock']:
                            total_addresses_geocoded += 1
                            
                    except Exception as e:
                        print(f"Error geocoding address for case {case_number}: {e}")
                        # Insert address without geocoding
                        address_records.append((
                            case_number,
                            addr['address_type'],
                            addr['address_line1'],
                            addr['address_line2'],
                            addr['city'],
                            addr['state'],
                            addr['country'],
                            addr['postal_code'],
                            None,
                            None,
                            'failed'
                        ))
                        
            except Exception as e:
                print(f"Error processing addresses for case {case_number}: {e}")
        
        # Bulk insert addresses
        if address_records:
            cur.executemany("""
                INSERT INTO address (case_number, address_type, address_line1, address_line2, city, state, country, postal_code, latitude, longitude, geocode_status)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, address_records)
            conn.commit()
            print(f"Processed batch {i // BATCH_SIZE + 1}: {len(address_records)} addresses from {len(batch)} cases")
    
    cur.close()
    conn.close()
    print(f"Address extraction and geocoding completed!")
    print(f"Total addresses processed: {total_addresses_processed}")
    print(f"Total addresses geocoded: {total_addresses_geocoded}")
    print(f"Geocoding success rate: {(total_addresses_geocoded/total_addresses_processed)*100:.1f}%" if total_addresses_processed > 0 else "No addresses to process")


def extract_addresses_from_html(html_content: str, case_number: str):
    """
    Extract addresses from HTML content using existing parsers.
    """
    addresses = []
    
    try:
        # Parse parties addresses
        parties = extract_parties_from_text(html_content)
        for party in parties:
            if party.get('address') and party.get('city'):
                # Parse state and zip from state_zip field
                state_zip = party.get('state_zip', '')
                state_zip_parts = state_zip.split()
                state = state_zip_parts[0] if state_zip_parts else ''
                postal_code = state_zip_parts[1] if len(state_zip_parts) > 1 else ''
                
                addresses.append({
                    'case_number': case_number,
                    'address_type': f"party_{party.get('type', 'unknown').lower()}",
                    'address_line1': party.get('address', '').strip(),
                    'address_line2': None,
                    'city': party.get('city', '').strip(),
                    'state': state.strip(),
                    'country': 'USA',
                    'postal_code': postal_code.strip()
                })
        
        # Parse attorney addresses  
        attorneys = extract_attorneys_from_text(html_content)
        for attorney in attorneys:
            if attorney.get('address') and attorney.get('city'):
                addresses.append({
                    'case_number': case_number,
                    'address_type': f"attorney_{attorney.get('party_type', 'unknown').lower()}",
                    'address_line1': attorney.get('address', '').strip(),
                    'address_line2': None,
                    'city': attorney.get('city', '').strip(),
                    'state': attorney.get('state_zip', '').strip(),
                    'country': 'USA',
                    'postal_code': ''
                })
                
    except Exception as e:
        print(f"Error extracting addresses from case {case_number}: {e}")
    
    return addresses

with DAG(
    dag_id="batch_cases_to_postgres",
    start_date=datetime(2025, 10, 22),
    schedule="@daily",
    catchup=False,
    description="Process HTML case files and extract/geocode addresses",
    tags=["etl", "cases", "geocoding"],
) as dag:

    # Task 1: Process HTML files and store raw case data
    task_batch_html = PythonOperator(
        task_id="batch_html_to_postgres",
        python_callable=batch_html_to_postgres,
        doc_md="""
        ### Batch HTML to PostgreSQL
        
        This task processes HTML case files and inserts them into the raw_cases table.
        - Reads HTML files from the cases directory
        - Extracts case numbers from filenames
        - Bulk inserts raw HTML content into PostgreSQL
        - Handles duplicates with ON CONFLICT clause
        """
    )

    # Task 2: Extract addresses and geocode them
    task_extract_geocode = PythonOperator(
        task_id="extract_and_geocode_addresses",
        python_callable=extract_and_geocode_addresses,
        doc_md="""
        ### Extract and Geocode Addresses
        
        This task extracts addresses from processed cases and geocodes them.
        - Reads raw HTML from cases that haven't been processed for addresses
        - Extracts party and attorney addresses using existing parsers
        - Geocodes addresses using mock service (returns 0,0 coordinates)
        - Stores addresses with geolocation data in address table
        
        **Mock Geocoding**: Currently returns (0,0) coordinates for all addresses.
        Future implementation will use Ohio State's geocoding service.
        """
    )

    # Set up task dependencies: HTML processing must complete before address extraction
    task_batch_html >> task_extract_geocode
