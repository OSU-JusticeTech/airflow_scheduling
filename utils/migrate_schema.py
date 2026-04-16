"""
Database migration utility for separating attorneys and parties tables
with proper case relationships and geocoded addresses.

This script helps migrate data from the old schema to the new schema.
"""

import psycopg2
import os
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_USER = os.getenv("DB_USER")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")

def migrate_address_data():
    """Migrate data from old address table to new addresses and geocoded_addresses tables"""
    
    conn = psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT
    )
    cur = conn.cursor()
    
    try:
        print("🔄 Starting address data migration...")
        
        # Check if old address table exists
        cur.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = 'address'
            );
        """)
        old_table_exists = cur.fetchone()[0]
        
        if not old_table_exists:
            print("✅ Old address table doesn't exist - migration not needed")
            return
        
        # Get all data from old address table
        cur.execute("""
            SELECT address_id, case_number, party_name, address_type, address_line1, address_line2,
                   city, state, country, postal_code, latitude, longitude, geocoded_at, geocode_status,
                   created_at
            FROM address
            ORDER BY address_id
        """)
        
        old_addresses = cur.fetchall()
        print(f"📊 Found {len(old_addresses)} addresses to migrate")
        
        migrated_count = 0
        skipped_count = 0
        
        for old_addr in old_addresses:
            (old_addr_id, case_number, party_name, address_type, address_line1, address_line2,
             city, state, country, postal_code, latitude, longitude, geocoded_at, geocode_status,
             created_at) = old_addr
            
            try:
                # Get case_id for this case_number
                cur.execute("SELECT case_id FROM cases WHERE case_number = %s LIMIT 1", (case_number,))
                case_result = cur.fetchone()
                
                if not case_result:
                    print(f"⚠️  No case found for case_number {case_number}, skipping address {old_addr_id}")
                    skipped_count += 1
                    continue
                
                case_id = case_result[0]
                
                # Insert into new addresses table
                cur.execute("""
                    INSERT INTO addresses (address_line1, address_line2, city, state, country, postal_code, created_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    RETURNING address_id
                """, (
                    address_line1,
                    address_line2,
                    city,
                    state or 'OH',
                    country or 'USA', 
                    postal_code,
                    created_at or datetime.now()
                ))
                
                new_address_id = cur.fetchone()[0]
                
                # Insert into geocoded_addresses table
                cur.execute("""
                    INSERT INTO geocoded_addresses (
                        address_id, entity_type, entity_id, case_id, latitude, longitude,
                        geocoded_at, geocode_status, geocode_service, created_at
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    new_address_id,
                    address_type or 'unknown',
                    0,  # Temporary entity_id - will be updated when parties/attorneys are linked
                    case_id,
                    latitude,
                    longitude,
                    geocoded_at,
                    geocode_status or 'migrated',
                    'legacy',
                    created_at or datetime.now()
                ))
                
                migrated_count += 1
                
                if migrated_count % 100 == 0:
                    print(f"   Migrated {migrated_count} addresses...")
                    conn.commit()
                    
            except Exception as e:
                print(f"❌ Error migrating address {old_addr_id}: {e}")
                continue
        
        conn.commit()
        print(f"✅ Migration complete: {migrated_count} addresses migrated, {skipped_count} skipped")
        
        # Optional: Rename old table instead of dropping it
        cur.execute("ALTER TABLE address RENAME TO address_backup_" + datetime.now().strftime("%Y%m%d_%H%M%S"))
        conn.commit()
        print("📦 Old address table backed up")
        
    except Exception as e:
        print(f"❌ Migration failed: {e}")
        conn.rollback()
    finally:
        cur.close()
        conn.close()


def create_sample_case_data():
    """Create sample cases, parties, and attorneys for testing the new schema"""
    
    conn = psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT
    )
    cur = conn.cursor()
    
    try:
        print("🔄 Creating sample case data...")
        
        # Create sample cases
        sample_cases = [
            ("2024-CV-001", "Smith vs. Jones", "Civil case", "Active"),
            ("2024-CR-002", "State vs. Johnson", "Criminal case", "Pending"),
            ("2024-CV-003", "ABC Corp vs. XYZ Inc", "Contract dispute", "Closed")
        ]
        
        case_ids = []
        for case_data in sample_cases:
            cur.execute("""
                INSERT INTO cases (case_number, case_title, case_description, case_status, pipeline_status)
                VALUES (%s, %s, %s, %s, 'valid')
                RETURNING case_id
            """, case_data)
            case_ids.append(cur.fetchone()[0])
        
        print(f"✅ Created {len(case_ids)} sample cases")
        
        # Create sample addresses
        sample_addresses = [
            ("123 Main St", None, "Columbus", "OH", "USA", "43215"),
            ("456 Oak Ave", "Apt 2B", "Cleveland", "OH", "USA", "44101"), 
            ("789 Elm Blvd", None, "Cincinnati", "OH", "USA", "45202")
        ]
        
        address_ids = []
        for addr_data in sample_addresses:
            cur.execute("""
                INSERT INTO addresses (address_line1, address_line2, city, state, country, postal_code)
                VALUES (%s, %s, %s, %s, %s, %s)
                RETURNING address_id
            """, addr_data)
            address_ids.append(cur.fetchone()[0])
        
        print(f"✅ Created {len(address_ids)} sample addresses")
        
        # Create sample parties
        sample_parties = [
            (case_ids[0], "John Smith", "Plaintiff", address_ids[0]),
            (case_ids[0], "Jane Jones", "Defendant", address_ids[1]),
            (case_ids[1], "State of Ohio", "Plaintiff", None),
            (case_ids[1], "Bob Johnson", "Defendant", address_ids[2]),
            (case_ids[2], "ABC Corporation", "Plaintiff", address_ids[0]),
            (case_ids[2], "XYZ Inc", "Defendant", address_ids[1])
        ]
        
        party_ids = []
        for party_data in sample_parties:
            cur.execute("""
                INSERT INTO party (case_id, party_name, party_type, address_id)
                VALUES (%s, %s, %s, %s)
                RETURNING party_id
            """, party_data)
            party_ids.append(cur.fetchone()[0])
        
        print(f"✅ Created {len(party_ids)} sample parties")
        
        # Create sample attorneys
        sample_attorneys = [
            (case_ids[0], "Sarah Attorney", party_ids[0], "Defense", address_ids[1]),
            (case_ids[0], "Mike Lawyer", party_ids[1], "Prosecution", address_ids[2]),
            (case_ids[1], "District Attorney Office", None, "Prosecution", None),
            (case_ids[1], "Public Defender", party_ids[3], "Defense", address_ids[0]),
            (case_ids[2], "Corporate Counsel", party_ids[4], "Corporate", address_ids[2])
        ]
        
        attorney_ids = []
        for attorney_data in sample_attorneys:
            cur.execute("""
                INSERT INTO attorney (case_id, attorney_name, party_id, attorney_type, address_id)
                VALUES (%s, %s, %s, %s, %s)
                RETURNING attorney_id
            """, attorney_data)
            attorney_ids.append(cur.fetchone()[0])
        
        print(f"✅ Created {len(attorney_ids)} sample attorneys")
        
        # Create sample geocoded addresses
        sample_geocoded = [
            (address_ids[0], 'party', party_ids[0], case_ids[0], 39.9612, -82.9988, 'success', 'sample'),
            (address_ids[1], 'party', party_ids[1], case_ids[0], 41.4993, -81.6944, 'success', 'sample'),
            (address_ids[1], 'attorney', attorney_ids[0], case_ids[0], 41.4993, -81.6944, 'success', 'sample'),
            (address_ids[2], 'party', party_ids[3], case_ids[1], 39.1031, -84.5120, 'success', 'sample')
        ]
        
        for geocoded_data in sample_geocoded:
            cur.execute("""
                INSERT INTO geocoded_addresses (
                    address_id, entity_type, entity_id, case_id, latitude, longitude, 
                    geocode_status, geocode_service, geocoded_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW())
            """, geocoded_data)
        
        print(f"✅ Created {len(sample_geocoded)} sample geocoded addresses")
        
        conn.commit()
        print("🎉 Sample data creation complete!")
        
    except Exception as e:
        print(f"❌ Error creating sample data: {e}")
        conn.rollback()
    finally:
        cur.close()
        conn.close()


def verify_schema():
    """Verify the new schema structure and relationships"""
    
    conn = psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT
    )
    cur = conn.cursor()
    
    try:
        print("🔍 Verifying new schema structure...")
        
        # Check table existence
        tables_to_check = ['addresses', 'geocoded_addresses', 'cases', 'party', 'attorney']
        for table in tables_to_check:
            cur.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_name = %s
                );
            """, (table,))
            exists = cur.fetchone()[0]
            print(f"   Table {table}: {'✅ EXISTS' if exists else '❌ MISSING'}")
        
        # Check record counts
        for table in tables_to_check:
            try:
                cur.execute(f"SELECT COUNT(*) FROM {table}")
                count = cur.fetchone()[0]
                print(f"   {table} records: {count}")
            except Exception as e:
                print(f"   {table} records: ❌ Error - {e}")
        
        # Check foreign key relationships
        cur.execute("""
            SELECT 
                tc.table_name, 
                kcu.column_name, 
                ccu.table_name AS foreign_table_name,
                ccu.column_name AS foreign_column_name 
            FROM 
                information_schema.table_constraints AS tc 
                JOIN information_schema.key_column_usage AS kcu
                    ON tc.constraint_name = kcu.constraint_name
                    AND tc.table_schema = kcu.table_schema
                JOIN information_schema.constraint_column_usage AS ccu
                    ON ccu.constraint_name = tc.constraint_name
                    AND ccu.table_schema = tc.table_schema
            WHERE tc.constraint_type = 'FOREIGN KEY' 
            AND tc.table_name IN ('party', 'attorney', 'geocoded_addresses')
            ORDER BY tc.table_name, kcu.column_name;
        """)
        
        foreign_keys = cur.fetchall()
        print("   Foreign Key Relationships:")
        for fk in foreign_keys:
            print(f"     {fk[0]}.{fk[1]} -> {fk[2]}.{fk[3]}")
        
        print("✅ Schema verification complete!")
        
    except Exception as e:
        print(f"❌ Error during verification: {e}")
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    print("🚀 Database Migration Utility")
    print("=============================")
    
    # Run migration steps
    migrate_address_data()
    create_sample_case_data()
    verify_schema()
    
    print("\n🎉 Migration utility complete!")
    print("\nNext steps:")
    print("1. Run the Alembic migration: alembic upgrade head")
    print("2. Test the updated ETL DAG")
    print("3. Verify data integrity")
