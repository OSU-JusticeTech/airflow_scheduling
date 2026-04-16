from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import psycopg2
from bs4 import BeautifulSoup
import sys
import re
import signal
import requests
from pathlib import Path
from requests.exceptions import Timeout, ConnectionError

# Ensure project root imports resolve in Airflow workers.
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

from config.runtime import CASES_FILE_DIRECTORY, DB_CONFIG, ensure_project_root_on_path

ensure_project_root_on_path()

try:
    from utils.XML_parser import parse_case_html, make_case
    from utils.pyschema import Case, SideAddress, Attorney, FakeAttorney, RunningAttorney, PublicAttorney
    XML_PARSER_AVAILABLE = True
except ImportError:
    print("ERROR: Could not import XML parser - import failed at module load time")
    XML_PARSER_AVAILABLE = False
from utils.load_parsed_data import parse_and_insert_from_db

BATCH_SIZE = 1000
# under public schema now 
TABLE_NAME = "raw_cases"


def batch_html_to_postgres():
    """Load HTML files to postgres and detect changes for re-processing"""
    if not CASES_FILE_DIRECTORY.exists():
        print(f"HTML source directory does not exist: {CASES_FILE_DIRECTORY}")
        return

    # Support both legacy flat folders and nested case folders.
    # Prefer printable.html from each case folder when available.
    nested_preferred = sorted(CASES_FILE_DIRECTORY.glob("*/printable.html"))
    if nested_preferred:
        html_files = nested_preferred
    else:
        flat_html_files = sorted(CASES_FILE_DIRECTORY.glob("*.html"))
        html_files = flat_html_files if flat_html_files else sorted(CASES_FILE_DIRECTORY.rglob("*.html"))

    total_files = len(html_files)
    print(f"Found {total_files} HTML files in {CASES_FILE_DIRECTORY}.")

    if total_files == 0:
        return
    
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    new_files = 0
    updated_files = 0
    unchanged_files = 0

    # process files in batches
    for i in range(0, total_files, BATCH_SIZE):
        batch = html_files[i:i+BATCH_SIZE]
        records_to_insert = []
        records_to_update = []

        for file_path in batch:
            try:
                with open(file_path, "r", encoding="utf-8") as f:
                    content = f.read()

                    if file_path.stem in {"printable", "overview", "journal"}:
                        case_number = file_path.parent.name
                    else:
                        case_number = file_path.stem
                    
                    # check if case already exists and if content has changed
                    cur.execute("""
                        SELECT raw_html
                        FROM raw_cases 
                        WHERE case_number = %s
                    """, (case_number,))
                    
                    existing_record = cur.fetchone()
                    
                    if existing_record is None:
                        records_to_insert.append((case_number, content))
                        new_files += 1
                        
                    elif existing_record[0] != content:
                        records_to_update.append((content, case_number))
                        updated_files += 1
                        
                    else:
                        unchanged_files += 1
                        
            except Exception as e:
                conn.rollback()
                print(f"Error processing {file_path}: {e}")

        # bulk insert new records
        if records_to_insert:
            cur.executemany("""
                INSERT INTO raw_cases (case_number, raw_html, ingested_at)
                VALUES (%s, %s, NOW())
            """, records_to_insert)
            
        # bulk update changed records
        if records_to_update:
            cur.executemany("""
                UPDATE raw_cases 
                SET raw_html = %s, ingested_at = NOW()
                WHERE case_number = %s
            """, records_to_update)

        conn.commit()
        print(f"Batch {i // BATCH_SIZE + 1} processed: {len(records_to_insert)} new, {len(records_to_update)} updated")

    # Final summary
    print(f"\n  HTML PROCESSING COMPLETE:")
    print(f"    Total files processed: {total_files}")
    print(f"    New files: {new_files}")
    print(f"    Changed files (will re-process): {updated_files}")
    print(f"    Unchanged files: {unchanged_files}")

    cur.close()
    conn.close()
    print("All batches processed successfully!")


def extract_and_geocode_addresses():
    """Extract new addresses AND geocode existing ones using CURA service"""
    import time

    def _next_id(cursor, table_name, id_column):
        cursor.execute(f"SELECT COALESCE(MAX({id_column}), 0) + 1 FROM {table_name}")
        return cursor.fetchone()[0]
    
    def timeout_handler(signum, frame):
        """Signal handler for geocoding timeout"""
        raise TimeoutError("CURA geocoding request timeout (10 seconds)")
    
    def is_po_box_address(address_line1):
        """Check if address is a P.O. Box (these don't geocode well)"""
        if not address_line1:
            return False
        # Use simple substring checks instead of regex for reliability
        address_upper = address_line1.upper()
        po_indicators = ['P.O. BOX', 'P.O BOX', 'PO BOX', 'POB', 'POBOX', 'P O BOX']
        return any(indicator in address_upper for indicator in po_indicators)
    
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    
    start_time = time.time()
    max_runtime = 4 * 60
    extraction_time_limit = 60
    geocoding_time_limit = 3 * 60  # Increased from 2.5 to 3 minutes for more geocoding  
    
    # First, get the total count of unprocessed cases
    cur.execute("""
        SELECT COUNT(*) 
        FROM raw_cases r
        WHERE NOT EXISTS (
            SELECT 1 FROM cases c WHERE c.case_number = r.case_number
        )
        AND r.raw_html IS NOT NULL
    """)
    
    total_unprocessed = cur.fetchone()[0]
    print(f"PROCESSING STATUS: {total_unprocessed} cases remaining to process")
    
    # PHASE 1: Extract addresses, parties, and attorneys from new cases
    if total_unprocessed > 0:
        print("PHASE 1: Extracting data from new cases...")
        
        # extract new addresses from unprocessed cases
        cur.execute("""
            SELECT r.case_number, r.raw_html 
            FROM raw_cases r
            WHERE NOT EXISTS (
                SELECT 1 FROM cases c WHERE c.case_number = r.case_number
            )
            AND r.raw_html IS NOT NULL
            LIMIT 5
        """)
        
        unprocessed_cases = cur.fetchall()
        print(f"Processing batch of {len(unprocessed_cases)} cases from {total_unprocessed} remaining")    
        total_extracted = 0
        total_parties = 0
        total_attorneys = 0
        failed_cases = []
        
        for case_number, raw_html in unprocessed_cases:
            if time.time() - start_time > extraction_time_limit:  
                print("Extraction time limit reached!")
                break
                
            try:
                # Extract data using XML parser (enforced - no fallback)
                case_data = extract_case_data_with_xml_parser(raw_html, case_number)
                
                if case_data:
                    # Create or get case
                    cur.execute("""
                        SELECT case_id FROM cases WHERE case_number = %s
                    """, (case_number,))
                    
                    case_result = cur.fetchone()
                    if not case_result:
                        next_case_id = _next_id(cur, "cases", "case_id")
                        cur.execute("""
                            INSERT INTO cases (case_id, case_number, case_title, case_status)
                            VALUES (%s, %s, %s, %s)
                        """, (next_case_id, case_number, case_data.get('case_title', f"Case {case_number}"), 'processing'))
                        case_id = next_case_id
                    else:
                        case_id = case_result[0]
                    
                    # Insert addresses (with duplicate check)
                    for addr_data in case_data.get('addresses', []):
                        address_line1 = addr_data.get('address_line1', '').strip()
                        city = addr_data.get('city', '').strip()
                        state = addr_data.get('state', 'OH').strip()
                        postal_code = addr_data.get('postal_code', '').strip()
                        
                        # Check if address already exists
                        cur.execute("""
                            SELECT address_id FROM address 
                            WHERE address_line1 = %s AND city = %s AND state = %s AND postal_code = %s
                            LIMIT 1
                        """, (address_line1, city, state, postal_code))
                        
                        existing_address = cur.fetchone()
                        if existing_address:
                            address_id = existing_address[0]
                            print(f"        Using existing address ID {address_id}: {address_line1}")
                        else:
                            # Insert new address
                            next_address_id = _next_id(cur, "address", "address_id")
                            cur.execute("""
                                INSERT INTO address (address_id, address_type, address_line1, address_line2, city, state, 
                                                     country, postal_code)
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                            """, (
                                next_address_id,
                                addr_data.get('entity_type'),
                                address_line1,
                                addr_data.get('address_line2', ''),
                                city,
                                state,
                                addr_data.get('country', 'USA'),
                                postal_code
                            ))
                            address_id = next_address_id
                            print(f"        Created new address ID {address_id}: {address_line1}")
                            total_extracted += 1
                        
                        # Store address_id for linking to entities
                        addr_data['address_id'] = address_id
                    
                    # Insert parties and link addresses (with duplicate check)
                    for party_data in case_data.get('parties', []):
                        party_name = party_data.get('name', '').strip()
                        party_type = party_data.get('type', '').strip()
                        
                        # Check if party already exists for this case with same name and address
                        party_address_id = None
                        
                        for addr_data in case_data.get('addresses', []):
                            if addr_data.get('entity_type') == 'party' and addr_data.get('entity_name') == party_name:
                                party_address_id = addr_data.get('address_id')
                                break
                        
                        cur.execute("""
                            SELECT party_id FROM party 
                            WHERE party_type = %s AND address_id IS NOT DISTINCT FROM %s
                            LIMIT 1
                        """, (party_type, party_address_id))
                        
                        existing_party = cur.fetchone()
                        if existing_party:
                            party_id = existing_party[0]
                            print(f"         Reusing existing party: {party_name} (ID {party_id})")
                        else:
                            # Insert new party
                            next_party_id = _next_id(cur, "party", "party_id")
                            cur.execute("""
                                INSERT INTO party (party_id, party_type, address_id, created)
                                VALUES (%s, %s, %s, %s)
                            """, (next_party_id, party_type, party_address_id, datetime.now()))
                            party_id = next_party_id
                            total_parties += 1
                            print(f"        Created new party: {party_name} (ID {party_id})")

                        # Ensure party is linked to case through association table.
                        cur.execute("""
                            SELECT 1 FROM case_party
                            WHERE case_id = %s AND party_id = %s
                            LIMIT 1
                        """, (case_id, party_id))
                        if not cur.fetchone():
                            cur.execute("""
                                INSERT INTO case_party (case_id, party_id)
                                VALUES (%s, %s)
                            """, (case_id, party_id))
                    
                    # Insert attorneys using normalized structure (with duplicate check)
                    for attorney_data in case_data.get('attorneys', []):
                        attorney_name = attorney_data.get('name', '').strip()
                        attorney_type = attorney_data.get('type', '').strip()
                        
                        # Find matching address for this attorney first
                        attorney_address_id = None
                        
                        for addr_data in case_data.get('addresses', []):
                            if addr_data.get('entity_type') == 'attorney' and addr_data.get('entity_name') == attorney_name:
                                attorney_address_id = addr_data.get('address_id')
                                break
                        
                        # STEP 1: Insert/find attorney as ENTITY (person) - normalized system without case_id
                        cur.execute("""
                            SELECT attorney_id FROM attorney
                            WHERE attorney_type = %s
                              AND address_id IS NOT DISTINCT FROM %s
                            LIMIT 1
                        """, (attorney_type, attorney_address_id))
                        
                        existing_attorney_entity = cur.fetchone()
                        if existing_attorney_entity:
                            attorney_entity_id = existing_attorney_entity[0]
                            print(f"         Reusing attorney entity: {attorney_name} (ID {attorney_entity_id})")
                        else:
                            # Insert new attorney row compatible with current schema.
                            next_attorney_id = _next_id(cur, "attorney", "attorney_id")
                            cur.execute("""
                                INSERT INTO attorney (attorney_id, party_id, attorney_type, address_id, created)
                                VALUES (%s, %s, %s, %s, %s)
                            """, (next_attorney_id, None, attorney_type, attorney_address_id, datetime.now()))
                            attorney_entity_id = next_attorney_id
                            total_attorneys += 1
                            print(f"        Created new attorney entity: {attorney_name} (ID {attorney_entity_id})")
                
            except Exception as e:
                # Skip malformed or parser-failing cases; continue with remaining work.
                conn.rollback()
                failed_cases.append((case_number, str(e)))
                try:
                    cur.execute("""
                        SELECT case_id
                        FROM cases
                        WHERE case_number = %s
                        LIMIT 1
                    """, (case_number,))
                    existing_case = cur.fetchone()

                    if existing_case:
                        cur.execute("""
                            UPDATE cases
                            SET case_status = %s,
                                case_title = COALESCE(case_title, %s)
                            WHERE case_number = %s
                        """, ('parse_failed', f"Case {case_number}", case_number))
                    else:
                        next_case_id = _next_id(cur, "cases", "case_id")
                        cur.execute("""
                            INSERT INTO cases (case_id, case_number, case_title, case_status)
                            VALUES (%s, %s, %s, %s)
                        """, (next_case_id, case_number, f"Case {case_number}", 'parse_failed'))

                    conn.commit()
                except Exception as quarantine_error:
                    conn.rollback()
                    print(f"ERROR recording parse failure for {case_number}: {quarantine_error}")
                print(f"ERROR processing case {case_number}: {e}")
                continue
        
        conn.commit()
        print(f"  PHASE 1 COMPLETE: {total_extracted} new addresses, {total_parties} new parties, {total_attorneys} new attorneys")
        if failed_cases:
            print(f"  Skipped {len(failed_cases)} case(s) due to parser/format errors")
            print("  Marked skipped cases with case_status=parse_failed")
            for failed_case_number, failed_reason in failed_cases[:5]:
                print(f"    - {failed_case_number}: {failed_reason}")
    else:
        print("  No new cases to process")
    
    # PHASE 2: Geocode existing addresses (with time limit)
    if time.time() - start_time < geocoding_time_limit:
        print("  PHASE 2: Geocoding addresses...")
        
        # Initialize CURA geocoding service
        try:
            from utils.geolocation import create_geolocation_service
            geo_service = create_geolocation_service()
        except Exception as e:
            print(f"Failed to initialize CURA geocoding service: {e}")
            cur.close()
            conn.close()
            return
        
        # Geocode only PARTY addresses
        cur.execute("""
            SELECT a.address_id, a.address_line1, a.city, a.state, a.postal_code,
                   'party' as entity_type, p.party_name as entity_name, a.created_at
            FROM addresses a
            JOIN party p ON a.address_id = p.address_id
            WHERE a.address_id NOT IN (
                SELECT DISTINCT address_id 
                FROM geocoded_addresses 
                WHERE geocode_status IN ('success', 'failed', 'skipped_po_box', 'timeout')
                AND address_id IS NOT NULL
            )
            AND a.address_line1 IS NOT NULL
            AND a.address_line1 != ''
            ORDER BY a.created_at ASC
            LIMIT 25
        """)
        
        addresses_to_geocode = cur.fetchall()
        geocoded_count = 0
        
        if addresses_to_geocode:
            print(f"Geocoding {len(addresses_to_geocode)} party addresses...")
            print(f"  CURA BATCH STARTED: {len(addresses_to_geocode)} addresses queued for geocoding")
            
            # Geocode each address individually
            for i, (address_id, address_line1, city, state, postal_code, entity_type, entity_name) in enumerate(addresses_to_geocode):
                # Check time limits
                if time.time() - start_time > max_runtime or time.time() - start_time > geocoding_time_limit:
                    print(f"  Time limit reached, stopping")
                    break
                
                if time.time() - start_time > (max_runtime * 0.8):
                    print(f"  Approaching max runtime, stopping")
                    break
                
                # Verify party-address relationship
                cur.execute("SELECT 1 FROM party WHERE address_id = %s LIMIT 1", (address_id,))
                if not cur.fetchone():
                    continue
                
                # Skip P.O. Box addresses - they don't geocode well
                if is_po_box_address(address_line1):
                    print(f"\n  [{i+1}/{len(addresses_to_geocode)}] {entity_name}: {address_line1}")
                    print(f"         SKIPPED: P.O. Box addresses cannot be geocoded")
                    try:
                        cur.execute("INSERT INTO geocoded_addresses (address_id, geocode_status, geocoded_at, geocode_service) VALUES (%s, %s, NOW(), %s) ON CONFLICT (address_id) DO UPDATE SET geocode_status = EXCLUDED.geocode_status", (address_id, 'skipped_po_box', 'CURA'))
                        conn.commit()
                    except:
                        pass
                    continue
                
                print(f"\n  [{i+1}/{len(addresses_to_geocode)}] {entity_name}: {address_line1}")
                
                try:
                    address_start = time.time()
                    
                    # Log CURA request
                    request_payload = {
                        'address_line1': address_line1,
                        'city': city if city != 'Unknown' else '',
                        'state': state or 'OH',
                        'postal_code': postal_code or ''
                    }
                    print(f"         CURA REQUEST SENT (Address ID {address_id}):")
                    print(f"           {request_payload}")

                    try:
                        # Use timeout on the geocoding call directly
                        result = geo_service.geocode_address(request_payload, timeout=5)
                    except (Timeout, ConnectionError, TimeoutError) as te:
                        geocode_time = time.time() - address_start
                        print(f"        CURA RESPONSE (TIMEOUT after {geocode_time:.2f}s)")
                        try:
                            cur.execute("INSERT INTO geocoded_addresses (address_id, geocode_status, geocoded_at, geocode_service) VALUES (%s, %s, NOW(), %s) ON CONFLICT (address_id) DO UPDATE SET geocode_status = EXCLUDED.geocode_status", (address_id, 'timeout', 'CURA'))
                            conn.commit()
                        except:
                            pass
                        continue
                    
                    geocode_time = time.time() - address_start
                    
                    # Log CURA response
                    if result['status'] == 'success':
                        lat = result.get('latitude', 'N/A')
                        lng = result.get('longitude', 'N/A')
                        print(f"        CURA RESPONSE SUCCESS ({geocode_time:.2f}s): Lat={lat}, Lng={lng}")
                        cur.execute("INSERT INTO geocoded_addresses (address_id, latitude, longitude, geocode_status, geocoded_at, geocode_service) VALUES (%s, %s, %s, %s, NOW(), %s) ON CONFLICT (address_id) DO UPDATE SET latitude = EXCLUDED.latitude, longitude = EXCLUDED.longitude, geocode_status = EXCLUDED.geocode_status", (address_id, result['latitude'], result['longitude'], 'success', 'CURA'))
                        geocoded_count += 1
                        print(f"        SUCCESS - Stored in database")
                    else:
                        status = result.get('status', 'unknown')
                        failed_status = 'failed' if status != 'success' else status
                        print(f"        CURA RESPONSE ({geocode_time:.2f}s): Status={status}")
                        cur.execute("INSERT INTO geocoded_addresses (address_id, geocode_status, geocoded_at, geocode_service) VALUES (%s, %s, NOW(), %s) ON CONFLICT (address_id) DO UPDATE SET geocode_status = EXCLUDED.geocode_status", (address_id, failed_status, 'CURA'))
                    
                    conn.commit()
                    time.sleep(0.1)
                        
                except Exception as e:
                    geocode_time = time.time() - address_start
                    print(f"      ERROR ({geocode_time:.2f}s): {type(e).__name__}: {e}")
                    try:
                        cur.execute("INSERT INTO geocoded_addresses (address_id, geocode_status, geocoded_at, geocode_service) VALUES (%s, %s, NOW(), %s) ON CONFLICT (address_id) DO UPDATE SET geocode_status = EXCLUDED.geocode_status", (address_id, 'error', 'CURA'))
                        conn.commit()
                    except:
                        pass
            
            print(f"  PHASE 2 COMPLETE: {geocoded_count} addresses geocoded")
        else:
            print("  No party addresses ready for geocoding")
    else:
        print("  Skipping geocoding phase - extraction took too long")
    
    conn.commit()
    
    # Final status report
    cur.execute("SELECT to_regclass('public.geocoded_addresses')")
    has_geocoded_addresses = cur.fetchone()[0] is not None
    if has_geocoded_addresses:
        cur.execute("SELECT geocode_status, COUNT(*) FROM geocoded_addresses GROUP BY geocode_status ORDER BY COUNT(*) DESC")
        status_counts = cur.fetchall()

        cur.execute("SELECT COUNT(*) FROM address a JOIN party p ON a.address_id = p.address_id WHERE a.address_id NOT IN (SELECT DISTINCT address_id FROM geocoded_addresses WHERE address_id IS NOT NULL)")
        remaining_addresses = cur.fetchone()[0]
    else:
        status_counts = [("skipped_missing_table", 0)]
        remaining_addresses = 0
    
    # Get final count of unprocessed case files
    cur.execute("""
        SELECT COUNT(*) 
        FROM raw_cases r
        WHERE NOT EXISTS (
            SELECT 1 FROM cases c WHERE c.case_number = r.case_number
        )
        AND r.raw_html IS NOT NULL
    """)
    remaining_files = cur.fetchone()[0]
    
    runtime = time.time() - start_time
    print(f"\n  DAG RUN SUMMARY:")
    print(f"    Runtime: {runtime:.1f} seconds")
    print(f"    Remaining cases to process: {remaining_files}")
    print(f"    Remaining addresses to geocode: {remaining_addresses}")
    print(f"    Geocoding status distribution:")
    for status, count in status_counts:
        print(f"      {status}: {count}")
    
    cur.close()
    conn.close()
    

def extract_case_data_with_xml_parser(html_content, case_number):
    """Extract comprehensive case data using XML parser only (no fallback).
    Raises RuntimeError if XML parser is unavailable or fails.
    """

    # Enforce XML parser availability
    if not XML_PARSER_AVAILABLE:
        raise RuntimeError(f"XML parser not available for {case_number} - import failed at DAG startup. Ensure utils.XML_parser is installed and importable.")

    try:
        print(f"    Extracting full case data for {case_number}")

        # Parse the case using XML parser
        parsed_data = parse_case_html(html_content)
        case_obj = make_case(parsed_data)

        # Enforce that parser returned a valid case object
        if case_obj is None:
            raise RuntimeError(f"XML parser returned None for {case_number} - parse_case_html or make_case failed")

        case_data = {
            'case_number': case_number,
            'case_title': getattr(case_obj, 'case_title', f"Case {case_number}"),
            'parties': [],
            'attorneys': [],
            'addresses': []
        }

        address_counter = 0

        # Extract parties and their addresses
        print(f"  Processing {len(case_obj.parties)} parties")
        for i, party in enumerate(case_obj.parties):
            party_name = getattr(party, 'name', f'Party {i+1}')
            party_type = 'Unknown'

            # Determine party type based on class or attributes
            if isinstance(party, SideAddress):
                party_type = getattr(party, 'side', 'Party')

            party_data = {
                'name': party_name,
                'type': party_type,
                'has_address': False,
                'address': None
            }

            # Extract party address if available
            if hasattr(party, 'address') and party.address:
                address_line = ' '.join(party.address) if isinstance(party.address, list) else str(party.address)

                if address_line and address_line.strip():
                    # Handle combined address format like "319 Oxford Oak Dr, Blacklick, Ohio, 43004"
                    street_address = address_line.strip()
                    city = getattr(party, 'city', 'Unknown')
                    state = getattr(party, 'state', 'OH')
                    postal_code = getattr(party, 'zip_', '')

                    # Parse combined address if it contains commas
                    if ',' in address_line:
                        try:
                            parts = [part.strip() for part in address_line.split(',')]
                            if len(parts) >= 4:
                                # "Street, City, State, ZIP" format
                                street_address = parts[0]
                                city = parts[1] if parts[1] != 'Unknown' else city
                                state = parts[2] if parts[2] != 'Unknown' else state
                                postal_code = parts[3] if parts[3] else postal_code
                            elif len(parts) == 3:
                                # "Street, City, State ZIP" format  
                                street_address = parts[0]
                                city = parts[1] if parts[1] != 'Unknown' else city
                                # Try to split state and ZIP from last part
                                state_zip = parts[2].strip()
                                match = re.match(r'^([A-Za-z\s]+?)\s*(\d{5}(?:-\d{4})?)$', state_zip)
                                if match:
                                    state = match.group(1).strip() if match.group(1).strip() != 'Unknown' else state
                                    postal_code = match.group(2)
                                else:
                                    state = state_zip if state_zip != 'Unknown' else state
                        except Exception as e:
                            print(f"    Warning: Could not parse combined address '{address_line}': {e}")

                    address_data = {
                        'address_line1': street_address,
                        'city': city,
                        'state': state,
                        'postal_code': postal_code,
                        'entity_type': 'party',
                        'entity_name': party_name
                    }
                    # Set address on party object for processing
                    party_data['has_address'] = True
                    party_data['address'] = {
                        'line1': street_address,
                        'city': city,
                        'state': state,
                        'postal_code': postal_code
                    }

                    case_data['addresses'].append(address_data)
                    address_counter += 1

            # Append party once (no duplicates)
            case_data['parties'].append(party_data)

        # Extract attorneys and their addresses
        print(f"  Processing {len(case_obj.attorneys)} attorneys")
        for i, attorney in enumerate(case_obj.attorneys):
            attorney_name = getattr(attorney, 'name', f'Attorney {i+1}')
            attorney_type = getattr(attorney, 'type', 'Attorney')

            # Skip fake attorneys
            try:
                if isinstance(attorney, FakeAttorney):
                    continue
            except NameError:
                # FakeAttorney not available, fallback to name-based check
                if "fake" in attorney_name.lower() or "placeholder" in attorney_name.lower():
                    continue

            # Try to determine which party this attorney represents
            representing_party = None
            if hasattr(attorney, 'party') and attorney.party:
                representing_party = getattr(attorney.party, 'name', None)

            attorney_data = {
                'name': attorney_name,
                'type': attorney_type,
                'representing_party': representing_party,
                'has_address': False,
                'address': None
            }

            # Extract attorney address if available
            if hasattr(attorney, 'address') and attorney.address:
                address_line = ' '.join(attorney.address) if isinstance(attorney.address, list) else str(attorney.address)

                # Skip placeholder addresses
                if ("***runners will pick up daily***" in address_line or 
                    "DO NOT USE" in address_line or 
                    not address_line.strip()):
                    continue

                # Extract or infer city/state/zip
                city = getattr(attorney, 'city', '')
                state = getattr(attorney, 'state', 'OH')
                postal_code = getattr(attorney, 'zip_', '')

                # Try to extract from address if missing
                if not city or not postal_code:
                    # Pattern for "City, State Zip"
                    city_state_zip_pattern = r'([A-Za-z\s]+),\s*([A-Z]{2})\s+(\d{5})'
                    match = re.search(city_state_zip_pattern, address_line)

                    if match:
                        city = city or match.group(1).strip()
                        state = state or match.group(2)
                        postal_code = postal_code or match.group(3)
                    else:
                        # Try to find zip pattern
                        zip_pattern = r'\b(\d{5})\b'
                        zip_match = re.search(zip_pattern, address_line)
                        if zip_match:
                            postal_code = postal_code or zip_match.group(1)

                        # Try to find common Ohio cities in the address
                        ohio_cities = [
                            'COLUMBUS', 'CLEVELAND', 'CINCINNATI', 'TOLEDO', 'AKRON', 'DAYTON', 'PARMA', 
                            'CANTON', 'YOUNGSTOWN', 'DUBLIN', 'WESTERVILLE', 'GROVE CITY', 'HILLIARD',
                            'REYNOLDSBURG', 'GAHANNA', 'UPPER ARLINGTON', 'BEXLEY', 'WORTHINGTON',
                            'CANAL WINCHESTER', 'PICKERINGTON', 'DELAWARE', 'BLACKLICK', 'GALLOWAY',
                            'ORIENT', 'LORAIN', 'HAMILTON', 'FAIRBORN', 'HUBBARD', 'FOREST', 'BELLEVUE'
                        ]
                        for ohio_city in ohio_cities:
                            if ohio_city in address_line.upper():
                                city = city or ohio_city.title()
                                break

                address_data = {
                    'address_line1': address_line,
                    'city': city or 'Unknown',
                    'state': state,
                    'postal_code': postal_code,
                    'entity_type': 'attorney',
                    'entity_name': attorney_name
                }

                # Set address on attorney object for processing
                attorney_data['has_address'] = True
                attorney_data['address'] = {
                    'line1': address_line,
                    'city': city or 'Unknown',
                    'state': state,
                    'postal_code': postal_code
                }

                case_data['addresses'].append(address_data)
                address_counter += 1

            case_data['attorneys'].append(attorney_data)

        print(f"  Extracted: {len(case_data['parties'])} parties, {len(case_data['attorneys'])} attorneys, {address_counter} addresses")
        return case_data

    except Exception as e:
        # Raise error (no fallback) so caller and Airflow can see the failure
        raise RuntimeError(f"XML parser failed for {case_number}: {e}")


def extract_addresses_simple(html_content, case_number):
    """Legacy simple extraction - kept for backwards compatibility if needed"""
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



def parse_staged_html_to_models(**context):
    dag_run = context.get("dag_run")
    dag_run_id = dag_run.run_id if dag_run else None
    task = context.get("task")
    task_id = task.task_id if task else None
    parse_and_insert_from_db(batch_size=BATCH_SIZE, dag_run_id=dag_run_id, task_id=task_id)

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

    task_parse_and_insert = PythonOperator(
        task_id="parse_html_to_models",
        python_callable=parse_staged_html_to_models,
    )
    
    task_extract_addresses = PythonOperator(
        task_id="extract_and_geocode_addresses", 
        python_callable=extract_and_geocode_addresses
    )

    # set task dependencies
    task_batch_html >> task_parse_and_insert >> task_extract_addresses
    
    