#!/usr/bin/env python3

"""
Simulation Script

This script processes new HTML case files and:
1. Extracts cases, parties, attorneys, and addresses using XML parsing
2. Geocode's party addresses automatically using CURA geolocation service
3. Stores data in a normalized database structure with duplicate prevention
"""

import os
import sys
import psycopg2
import requests
import time
from datetime import datetime
from dotenv import load_dotenv

# Add project root to path
sys.path.append('/Users/dishapatel/airflow_scheduling')

XML_PARSER_AVAILABLE = False
try:
    # First try direct import
    from utils.XML_parser import parse_case_html, make_case
    from utils.pyschema import Case, SideAddress, Attorney
    XML_PARSER_AVAILABLE = True
    print("XML parser loaded successfully (direct import)")
except ImportError:
    try:
        # alternative path
        import sys
        sys.path.insert(0, '/Users/dishapatel/airflow_scheduling/utils')
        from XML_parser import parse_case_html, make_case
        from pyschema import Case, SideAddress, Attorney
        XML_PARSER_AVAILABLE = True
        print("XML parser loaded successfully (alternative path)")
    except ImportError:
        try:
            # importing from current directory
            import os
            current_dir = os.path.dirname(os.path.abspath(__file__))
            utils_path = os.path.join(current_dir, 'utils')
            if utils_path not in sys.path:
                sys.path.append(utils_path)
            from XML_parser import parse_case_html, make_case
            from pyschema import Case, SideAddress, Attorney
            XML_PARSER_AVAILABLE = True
            print("XML parser loaded successfully (utils path)")
        except ImportError as e:
            print(f"❌ Warning: XML parser not available - {e}")
            XML_PARSER_AVAILABLE = False

load_dotenv()

# Configuration
CASES_FILE_DIRECTORY = os.path.expanduser("~/JusticeTech/cases")

def get_database_connection():
    """Get database connection"""
    return psycopg2.connect(
        dbname=os.getenv('DB_NAME'),
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD'),
        host=os.getenv('DB_HOST'),
        port=os.getenv('DB_PORT')
    )

def find_new_html_files():
    """Find HTML files that haven't been processed yet"""
    
    conn = get_database_connection()
    cur = conn.cursor()
    
    try:
        # Get list of all HTML files
        if not os.path.exists(CASES_FILE_DIRECTORY):
            print(f"❌ Cases directory not found: {CASES_FILE_DIRECTORY}")
            return []
            
        html_files = [f for f in os.listdir(CASES_FILE_DIRECTORY) if f.endswith(".html")]
        
        # Check what columns exist in raw_cases table
        cur.execute("""
            SELECT column_name FROM information_schema.columns 
            WHERE table_name = 'raw_cases' AND table_schema = 'public'
        """)
        columns = [row[0] for row in cur.fetchall()]
        print(f"Raw_cases table columns: {columns}")
        
        # Get processed case numbers from raw_cases table 
        processed_case_numbers = set()
        cur.execute("SELECT DISTINCT case_number FROM raw_cases")
        processed_case_numbers = set(row[0] for row in cur.fetchall())
        
        # Extract case numbers from filenames and find unprocessed ones
        new_files = []
        for filename in html_files:
            case_number = filename.replace('.html', '')
            if case_number not in processed_case_numbers:
                new_files.append(filename)
        
        print(f"📁 Found {len(html_files)} total HTML files")
        print(f"✅ Already processed: {len(processed_case_numbers)} files")
        print(f"🆕 New files to process: {len(new_files)} files")
        
        return new_files
        
    finally:
        cur.close()
        conn.close()

def parse_html_file(filename):
    
    filepath = os.path.join(CASES_FILE_DIRECTORY, filename)
    
    try:
        with open(filepath, 'r', encoding='utf-8') as file:
            html_content = file.read()
        
        print(f"   File size: {len(html_content)} characters")
        case_number = filename.replace('.html', '')
        
        # Extract case data using XML parser
        case_data = extract_case_data_with_xml_parser(html_content, case_number)
        
        if case_data:
            return case_data
        else:
            print(f"   ❌ No case data extracted")
            return None
            
    except Exception as e:
        print(f"   ❌ Error parsing {filename}: {e}")
        return None


def extract_case_data_with_xml_parser(html_content, case_number):
    
    if not XML_PARSER_AVAILABLE:
        print(f"  ❌ XML parser not available for {case_number}")
        return None
    
    try:        
        # Parse the case using XML parser
        parsed_data = parse_case_html(html_content)
        case_obj = make_case(parsed_data)
        
        if case_obj is None:
            print(f"  ❌ XML parser returned None for {case_number}")
            return None
        
        case_data = {
            'case_number': case_number,
            'case_title': getattr(case_obj, 'case_title', f"Case {case_number}"),
            'parties': [],
            'attorneys': [],
            'addresses': []
        }
        
        address_counter = 0
        
        # Extract parties and their addresses
        print(f"  \nProcessing {len(case_obj.parties)} parties")
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
                                import re
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
            
            case_data['parties'].append(party_data)
        
        # Extract attorneys and their addresses
        print(f"  Processing {len(case_obj.attorneys)} attorneys")
        for i, attorney in enumerate(case_obj.attorneys):
            attorney_name = getattr(attorney, 'name', f'Attorney {i+1}')
            attorney_type = getattr(attorney, 'type', 'Attorney')
            
            # Skip fake attorneys
            try:
                from utils.pyschema import FakeAttorney
                if isinstance(attorney, FakeAttorney):
                    continue
            except ImportError:
                # If FakeAttorney not available, check by name patterns
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
            case_data['attorneys'].append(attorney_data)
            
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
                    import re
                    
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
        
        print(f"  Extracted: {len(case_data['parties'])} parties, {len(case_data['attorneys'])} attorneys, {address_counter} addresses")
        return case_data
        
    except Exception as e:
        print(f"  ❌ XML parser failed for {case_number}: {e}")
        return None


def insert_or_get_address(address_data, conn):
    
    cur = conn.cursor()
    
    try:
        # Check if address already exists
        cur.execute("""
            SELECT address_id FROM addresses 
            WHERE address_line1 = %s AND city = %s AND state = %s AND postal_code = %s
            LIMIT 1
        """, (
            address_data.get('line1', '').strip(),
            address_data.get('city', '').strip(), 
            address_data.get('state', '').strip(),
            address_data.get('postal_code', '').strip()
        ))
        
        existing = cur.fetchone()
        if existing:
            return existing[0]
            
        # Insert new address
        cur.execute("""
            INSERT INTO addresses (address_line1, city, state, postal_code)
            VALUES (%s, %s, %s, %s)
            RETURNING address_id
        """, (
            address_data.get('line1', '').strip(),
            address_data.get('city', '').strip(),
            address_data.get('state', '').strip(), 
            address_data.get('postal_code', '').strip()
        ))
        
        result = cur.fetchone()
        return result[0] if result else None
        
    except Exception as e:
        print(f"Error inserting/getting address: {e}")
        return None
    finally:
        cur.close()


def insert_or_get_attorney(attorney_data, address_id, conn):
    
    cur = conn.cursor()
    
    try:
        attorney_name = attorney_data.get('name', '').strip()
        
        # Check if attorney entity already exists (same name + address)
        cur.execute("""
            SELECT attorney_id FROM attorneys 
            WHERE attorney_name = %s AND address_id = %s
            LIMIT 1
        """, (attorney_name, address_id))
        
        existing = cur.fetchone()
        if existing:
            return existing[0]
        
        # Insert new attorney entity
        cur.execute("""
            INSERT INTO attorneys (attorney_name, address_id, created)
            VALUES (%s, %s, %s)
            RETURNING attorney_id
        """, (attorney_name, address_id, datetime.now()))
        
        attorney_id = cur.fetchone()[0]
        print(f"   Created new attorney entity: {attorney_name} (ID {attorney_id})")
        return attorney_id
        
    finally:
        cur.close()

def create_case_attorney_relationship(case_id, attorney_id, attorney_type, conn):
    
    cur = conn.cursor()
    
    try:
        # Check if relationship already exists
        cur.execute("""
            SELECT case_attorney_id FROM case_attorneys 
            WHERE case_id = %s AND attorney_id = %s AND attorney_type = %s
            LIMIT 1
        """, (case_id, attorney_id, attorney_type))
        
        if cur.fetchone():
            return
        
        # Create new relationship
        cur.execute("""
            INSERT INTO case_attorneys (case_id, attorney_id, attorney_type, created)
            VALUES (%s, %s, %s, %s)
        """, (case_id, attorney_id, attorney_type, datetime.now()))
        
        print(f"   Created relationship: Case {case_id} ↔ Attorney {attorney_id} ({attorney_type})")
        
    finally:
        cur.close()

def insert_party(party_data, address_id, case_id, conn):
    
    cur = conn.cursor()
    
    try:
        party_name = party_data.get('name', '').strip()[:255]
        party_type = party_data.get('type', 'UNKNOWN').strip()[:100]
        
        # Check if party already exists for this case with same name and address
        cur.execute("""
            SELECT party_id FROM party 
            WHERE case_id = %s AND party_name = %s AND address_id = %s
            LIMIT 1
        """, (case_id, party_name, address_id))
        
        existing = cur.fetchone()
        if existing:
            return existing[0]
        
        # Check what columns exist in party table
        cur.execute("""
            SELECT column_name FROM information_schema.columns 
            WHERE table_name = 'party' AND table_schema = 'public'
        """)
        party_columns = [row[0] for row in cur.fetchall()]
        
        # Insert new party with appropriate columns
        if 'created_at' in party_columns:
            cur.execute("""
                INSERT INTO party (case_id, party_name, party_type, address_id, created_at)
                VALUES (%s, %s, %s, %s, %s)
                RETURNING party_id
            """, (
                case_id,
                party_name,
                party_type,
                address_id,
                datetime.now()
            ))
        elif 'created' in party_columns:
            cur.execute("""
                INSERT INTO party (case_id, party_name, party_type, address_id, created)
                VALUES (%s, %s, %s, %s, %s)
                RETURNING party_id
            """, (
                case_id,
                party_name,
                party_type,
                address_id,
                datetime.now()
            ))
        else:
            cur.execute("""
                INSERT INTO party (case_id, party_name, party_type, address_id)
                VALUES (%s, %s, %s, %s)
                RETURNING party_id
            """, (
                case_id,
                party_name,
                party_type,
                address_id
            ))
        
        party_id = cur.fetchone()[0]
        print(f"   Created new party: {party_name} (ID {party_id})")
        return party_id
        
    finally:
        cur.close()

def geocode_address(address_id, address_text, conn):
    """Geocode an address using CURA geolocation service"""
    
    cur = conn.cursor()
    
    try:
        # Check what columns exist in geocoded_addresses table
        cur.execute("""
            SELECT column_name FROM information_schema.columns 
            WHERE table_name = 'geocoded_addresses' AND table_schema = 'public'
        """)
        geo_columns = [row[0] for row in cur.fetchall()]
        
        # Check if already successfully geocoded
        cur.execute("""
            SELECT address_id, latitude, longitude, geocode_status
            FROM geocoded_addresses 
            WHERE address_id = %s 
              AND latitude IS NOT NULL 
              AND longitude IS NOT NULL 
              AND geocoded_at IS NOT NULL
              AND geocode_status = 'success'
            LIMIT 1
        """, (address_id,))
        
        existing = cur.fetchone()
        if existing:
            addr_id, lat, lng, status = existing
            print(f"         Address {address_id} already geocoded: ({lat}, {lng}) [{status}]")
            return True
        
        # Check if we've already tried recently and failed
        cur.execute("""
            SELECT geocode_status, geocoded_at
            FROM geocoded_addresses 
            WHERE address_id = %s 
              AND geocoded_at > NOW() - INTERVAL '1 day'
              AND geocode_status IN ('failed', 'error')
            LIMIT 1
        """, (address_id,))
        
        recent_failure = cur.fetchone()
        if recent_failure:
            geocode_status, geocoded_at = recent_failure
            print(f"         Address {address_id} failed geocoding recently ({geocode_status} at {geocoded_at}), skipping")
            return False
        
        # Delete any incomplete geocoding records first
        cur.execute("DELETE FROM geocoded_addresses WHERE address_id = %s", (address_id,))
        
        # Initialize CURA geocoding service
        print(f"         Geocoding: {address_text[:60]}...")
        
        try:
            from utils.geolocation import create_geolocation_service
            geo_service = create_geolocation_service()
        except Exception as e:
            print(f"         ❌ Failed to initialize CURA geocoding service: {e}")
            return False
        
        # Parse address text into components
        address_components = {
            'full_address': address_text.strip()
        }
        
        # Try to extract city, state, zip if present
        import re
        
        # Look for state and ZIP pattern: "OH 43215" or "Ohio 43215"
        state_zip_match = re.search(r'\b(OH|OHIO)\s+(\d{5})\b', address_text.upper())
        if state_zip_match:
            address_components['state'] = 'OH'
            address_components['postal_code'] = state_zip_match.group(2)
        
        # Look for city names
        for city in ['COLUMBUS', 'GROVE CITY', 'DUBLIN', 'WESTERVILLE', 'HILLIARD', 'CANAL WINCHESTER', 'WHITEHALL', 'BLACKLICK']:
            if city in address_text.upper():
                address_components['city'] = city.title()
                break
        
        # Call geocoding service
        try:
            result = geo_service.geocode_address(address_components)
            
            if result.get('status') == 'success':
                print(f"   Geocoding Results:")
                print(f"       Original: {address_text}")
                print(f"       Coordinates: ({result.get('latitude')}, {result.get('longitude')})")
                
                # Insert geocoded result using simplified schema
                cur.execute("""
                    INSERT INTO geocoded_addresses (address_id, latitude, longitude, geocoded_at, geocode_status, geocode_service)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, (
                    address_id,
                    result.get('latitude'),
                    result.get('longitude'),
                    datetime.now(),
                    'success',
                    'CURA'
                ))
                
                print(f"         Geocoded successfully and saved to database")
                return True
            else:
                print(f"         Geocoding failed: {result.get('status', 'Unknown error')}")
                
                cur.execute("""
                    INSERT INTO geocoded_addresses (address_id, geocoded_at, geocode_status, geocode_service)
                    VALUES (%s, %s, %s, %s)
                """, (address_id, datetime.now(), 'failed', 'CURA'))
                
                return False
                
        except Exception as geocode_error:
            print(f"         ❌ Geocoding service error: {geocode_error}")
            
            # Insert error status
            cur.execute("""
                INSERT INTO geocoded_addresses (address_id, geocoded_at, geocode_status, geocode_service)
                VALUES (%s, %s, %s, %s)
            """, (address_id, datetime.now(), 'error', 'CURA'))
            
            return False
            
    except Exception as e:
        print(f"         ❌ Geocoding database error: {e}")
        return False
    finally:
        cur.close()

def process_case_file(filename):
    """Process a single case file"""
    
    print(f"\nProcessing: {filename}")
    
    # Parse the HTML file
    case_data = parse_html_file(filename)
    if not case_data:
        print(f"   ❌ Failed to parse {filename}")
        return False
    
    conn = get_database_connection()
    
    try:
        # Start transaction
        cur = conn.cursor()
        
        # 1. Insert or get case
        cur.execute("""
            SELECT case_id FROM cases WHERE case_number = %s LIMIT 1
        """, (case_data['case_number'],))
        
        existing_case = cur.fetchone()
        if existing_case:
            case_id = existing_case[0]
            print(f"   \nUsing existing case: {case_data['case_number']} (ID {case_id})")
        else:
            # Check what columns exist in cases table
            cur.execute("""
                SELECT column_name FROM information_schema.columns 
                WHERE table_name = 'cases' AND table_schema = 'public'
            """)
            case_columns = [row[0] for row in cur.fetchall()]
            
            # Insert case with appropriate columns
            if 'created_at' in case_columns:
                cur.execute("""
                    INSERT INTO cases (case_number, case_title, created_at)
                    VALUES (%s, %s, %s)
                    RETURNING case_id
                """, (case_data['case_number'], case_data.get('case_title', ''), datetime.now()))
            elif 'created' in case_columns:
                cur.execute("""
                    INSERT INTO cases (case_number, case_title, created)
                    VALUES (%s, %s, %s)
                    RETURNING case_id
                """, (case_data['case_number'], case_data.get('case_title', ''), datetime.now()))
            else:
                # Just case_number and case_title
                cur.execute("""
                    INSERT INTO cases (case_number, case_title)
                    VALUES (%s, %s)
                    RETURNING case_id
                """, (case_data['case_number'], case_data.get('case_title', '')))
            
            case_id = cur.fetchone()[0]
            print(f"   Created new case: {case_data['case_number']} (ID {case_id})")
        
        # 2. Insert raw_cases record (track processed files)
        cur.execute("""
            INSERT INTO raw_cases (case_number, raw_html, created_at, content_hash, last_updated)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (case_number) DO UPDATE SET
                last_updated = EXCLUDED.last_updated
        """, (
            case_data['case_number'], 
            '', 
            datetime.now(),
            'manual_batch_' + str(datetime.now().timestamp()),
            datetime.now()
        ))
        
        # 3. Process attorneys (new normalized system)
        attorneys_processed = 0
        print(f"   Processing {len(case_data.get('attorneys', []))} attorneys...")
        
        for i, attorney in enumerate(case_data.get('attorneys', []), 1):
            print(f"      Attorney {i}: {attorney.get('name', 'N/A')} ({attorney.get('type', 'N/A')})")
            
            if attorney.get('has_address') and attorney.get('address'):
                # Insert/get address
                address_id = insert_or_get_address(attorney['address'], conn)
                print(f"         Address ID: {address_id}")
                
                # Insert/get attorney entity (reuses same ID for same person+address)
                attorney_id = insert_or_get_attorney(attorney, address_id, conn)
                
                # Create case-attorney relationship
                create_case_attorney_relationship(
                    case_id, attorney_id, attorney.get('type', 'UNKNOWN'), conn
                )
                attorneys_processed += 1
            else:
                print(f"         No address found for attorney {attorney.get('name', 'N/A')}")

        # 4. Process parties and geocode their addresses
        parties_processed = 0
        geocoded_count = 0
        print(f"   Processing {len(case_data.get('parties', []))} parties...")

        for i, party in enumerate(case_data.get('parties', []), 1):
            print(f"      Party {i}: {party.get('name', 'N/A')} ({party.get('type', 'N/A')})")
            
            if party.get('has_address') and party.get('address'):
                # Insert/get address
                address_id = insert_or_get_address(party['address'], conn)
                print(f"         Address ID: {address_id}")
                
                # Insert party
                party_id = insert_party(party, address_id, case_id, conn)
                
                # Only geocode if party was successfully created/found and address exists
                if party_id and address_id:
                    # Verify the party-address relationship exists before geocoding
                    cur.execute("""
                        SELECT 1 FROM party 
                        WHERE party_id = %s AND address_id = %s AND case_id = %s
                        LIMIT 1
                    """, (party_id, address_id, case_id))
                    
                    if cur.fetchone():
                        # Only geocode if party-address relationship confirmed
                        address_text = f"{party['address'].get('line1', '')} {party['address'].get('city', '')} {party['address'].get('state', '')} {party['address'].get('postal_code', '')}"
                        if geocode_address(address_id, address_text.strip(), conn):
                            geocoded_count += 1
                    else:
                        print(f"         Skipping geocoding - party-address relationship not confirmed")
                else:
                    print(f"         Skipping geocoding - party_id ({party_id}) or address_id ({address_id}) is None")

                parties_processed += 1
                # Small delay between geocoding requests
                time.sleep(0.5)
            else:
                print(f"         No address found for party {party.get('name', 'N/A')}")

        conn.commit()
        print(f"   \nSuccessfully processed {filename}")
        return True
        
    except Exception as e:
        conn.rollback()
        print(f"   ❌ Error processing {filename}: {e}")
        return False
    finally:
        conn.close()

def main():
    """Main processing function"""
    
    print("\nSIMULATION")
    print("=" * 60)
    
    # Processing mode selection
    print(f"\nPROCESSING MODE:")
    print(f"  1. Process only new/unprocessed files")
    print(f"  2. Force re-process specific files")
    print(f"  3. Force re-process a batch of files")

    try:
        mode_choice = input("\nSelect mode (1-3): ").strip()
    except (EOFError, KeyboardInterrupt):
        mode_choice = "1"
        print("1  # (defaulting to new files only)")
    
    if mode_choice == "2":
        # Process specific files
        conn = get_database_connection()
        cur = conn.cursor()
        
        # Show some sample files
        html_files = [f for f in os.listdir(CASES_FILE_DIRECTORY) if f.endswith(".html")][:10]
        print(f"\nSample files (first 10):")
        for i, f in enumerate(html_files):
            print(f"  {i+1}. {f}")
        
        try:
            test_files = input("\nEnter filenames to test (comma-separated) or press Enter to use first file: ").strip()
            if not test_files:
                files_to_process = [html_files[0]] if html_files else []
            else:
                files_to_process = [f.strip() for f in test_files.split(',')]
        except (EOFError, KeyboardInterrupt):
            files_to_process = [html_files[0]] if html_files else []
            print(f"{html_files[0] if html_files else 'None'}  # (defaulting to first file)")
        
        cur.close()
        conn.close()
        
    elif mode_choice == "3":
        # Force process a batch
        try:
            batch_size = int(input("How many files to process? ").strip())
        except (EOFError, KeyboardInterrupt, ValueError):
            batch_size = 5
            print("5  # (defaulting to 5 files)")
        
        html_files = [f for f in os.listdir(CASES_FILE_DIRECTORY) if f.endswith(".html")]
        files_to_process = html_files[:batch_size]
        
    else:
        # Process new files only
        new_files = find_new_html_files()
        
        if not new_files:
            print("\nNo new files to process!")
            print("💡 Use mode 2 or 3 to force re-process files for testing or geocoding")
            return
        
        # Limit to batch size if needed
        batch_size = 25
        if len(new_files) > batch_size:
            print(f"\nFound {len(new_files)} new files, limiting to batch of {batch_size}")
            new_files = new_files[:batch_size]
        
        files_to_process = new_files
    
    print(f"\nFiles to process: {files_to_process[:5]}{'...' if len(files_to_process) > 5 else ''}")
    
    # Confirm processing
    try:
        confirm = input(f"\nProcess {len(files_to_process)} files? (y/N): ").strip().lower()
        if confirm not in ['y', 'yes']:
            print("❌ Processing cancelled.")
            return
    except (EOFError, KeyboardInterrupt):
        print("\n❌ Processing cancelled.")
        return
    
    # Process files
    print(f"\n🔄 Processing {len(files_to_process)} files...")
    start_time = time.time()
    
    success_count = 0
    error_count = 0
    
    for i, filename in enumerate(files_to_process, 1):
        print(f"\n[{i}/{len(files_to_process)}]", end="")
        
        if process_case_file(filename):
            success_count += 1
        else:
            error_count += 1
        
    elapsed_time = time.time() - start_time
    
    # Summary
    print(f"\nPROCESSING SUMMARY:")
    print(f"   Successful: {success_count}")
    print(f"   Errors: {error_count}")
    print(f"   Total processed: {success_count + error_count}")

    if success_count > 0:
        # Show some stats
        conn = get_database_connection()
        cur = conn.cursor()
        try:
            cur.execute("SELECT COUNT(*) FROM attorneys")
            total_attorneys = cur.fetchone()[0]
            
            cur.execute("SELECT COUNT(*) FROM case_attorneys")
            total_relationships = cur.fetchone()[0]
            
            cur.execute("SELECT COUNT(*) FROM geocoded_addresses")
            total_geocoded = cur.fetchone()[0]
            
        finally:
            cur.close()
            conn.close()

if __name__ == "__main__":
    main()