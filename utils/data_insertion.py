import logging
import os
from dotenv import load_dotenv
import psycopg2

load_dotenv()

DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_USER = os.getenv("DB_USER")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")

LOG = logging.getLogger(__name__)
LOG.setLevel(logging.INFO)

def create_postgres_config():
    return {
        "user": DB_USER,
        "password": DB_PASSWORD,
        "host": DB_HOST,
        "port": DB_PORT,
        "database": DB_NAME,
    }

# ------------------------
# Insertion helpers
# ------------------------

def insert_case(cur, parsed, dag_run_id):
    LOG.info("[checkpoint] inserting case: %s", parsed.get("case_number"))
    cur.execute("""
        INSERT INTO cases (
            case_number, case_title, case_description, case_status, case_filed_date, created_dag_run_id
        )
        VALUES (%s, %s, %s, %s, %s, %s)
        RETURNING case_id
    """, (
        parsed.get("case_number"),
        parsed.get("case_title"),
        parsed.get("case_description"),
        parsed.get("case_status"),
        parsed.get("case_filed_date"),
        dag_run_id
    ))
    case_id = cur.fetchone()[0]
    LOG.info("[checkpoint] case inserted with id: %s", case_id)
    return case_id


# update cases table to updated_task_id field & case_status for efficient querying -- task_is UPDATED
def update_pipeline_status_to_valid(cur, case_ids, task_id):
    if not case_ids:
        return

    # Convert the list of IDs into a format suitable for the WHERE clause
    id_list_str = tuple(case_ids)
    
    # 💡 SQL Code for Status Update:
    cur.execute("""
        UPDATE cases
        SET
            pipeline_status = %s,
            updated_task_id = %s
        WHERE
            case_id = ANY(%s)
    """, (
        'Valid',
        task_id, 
        id_list_str
    ))
    
# TESTING UPDATE FOR SINGLE CASE ID
def update_pipeline_status_single(cur, case_id, new_pipeline_status, task_id):
    """Updates the pipeline status for a single case ID."""
    cur.execute("""
        UPDATE cases
        SET
            pipeline_status = %s,
            updated_task_id = %s
        WHERE
            case_id = %s
    """, (
        new_pipeline_status,
        task_id,
        case_id  # <--- No list/tuple conversion needed here!
    ))

def insert_address(cur, addr_dict, addr_type=None):
    if not addr_dict:
        return None
    
    # split state_zip if exists
    state = postal_code = None
    state_zip = addr_dict.get("state_zip")
    
    if state_zip and state_zip.strip():
        parts = state_zip.split("/", 1)
        state = parts[0].strip()
        postal_code = parts[1].strip() if len(parts) > 1 else None

    # use address_line1 and address if provided
    address_line1 = addr_dict.get("address_line1") or addr_dict.get("address")
    
    cur.execute("""
        INSERT INTO address (
            address_type, address_line1, address_line2, city, state, postal_code, country
        ) VALUES (%s, %s, %s, %s, %s, %s, %s)
        RETURNING address_id
    """, (
        addr_type,
        address_line1,
        addr_dict.get("address_line2"),
        addr_dict.get("city"),
        state,
        postal_code,
        addr_dict.get("country", "USA")
    ))
    return cur.fetchone()[0]

def insert_party(cur, case_id, party_data):
    
    # address details nested in party_data under 'address' key
    # address_dict_to_insert = party_data.get('address')
    
    # insert address first
    addr_id = insert_address(cur, party_data, addr_type="party")
    
    LOG.info("[checkpoint] inserting party for case_id: %s", case_id)
    cur.execute("""
        INSERT INTO party (party_type, party_name, address_id)
        VALUES (%s, %s, %s)
        RETURNING party_id
    """, (party_data.get("type"), party_data.get("name"), addr_id))
    party_id = cur.fetchone()[0]

    cur.execute("INSERT INTO case_party (case_id, party_id) VALUES (%s, %s)", (case_id, party_id))
    LOG.info("[checkpoint] party inserted with id: %s", party_id)
    return party_id


def insert_attorney(cur, case_id, atty_data):
    if not atty_data:
        LOG.debug("[debug] no attorney data found for case_id: %s", case_id)
        return None
    
    # insert address first
    # address_dict_to_insert = atty_data.get('address')
    addr_id = insert_address(cur, atty_data, addr_type="attorney")
    
    LOG.info("[checkpoint] inserting attorney party for case_id: %s", case_id)
    cur.execute("""
        INSERT INTO party (party_type, party_name, address_id)
        VALUES (%s, %s, %s)
        RETURNING party_id
    """, (atty_data.get("party_type"), atty_data.get("party_name"), addr_id))
    party_id = cur.fetchone()[0]

    LOG.info("[checkpoint] inserting attorney details for party_id: %s", party_id)
    cur.execute("""
        INSERT INTO attorney (party_id, attorney_type, attorney_name, address_id)
        VALUES (%s, %s, %s, %s)
        RETURNING attorney_id
    """, (party_id, atty_data.get("party_type"), atty_data.get("attorney_name"), addr_id))
    atty_id = cur.fetchone()[0]
    LOG.info("[checkpoint] attorney inserted with id: %s", atty_id)
    return atty_id

def insert_disposition(cur, case_id, disp_data):
    if not disp_data:
        LOG.debug("[debug] no disposition data found for case_id: %s", case_id)
        return None
    
    LOG.info("[checkpoint] inserting disposition for case_id: %s", case_id)
    cur.execute("""
        INSERT INTO disposition (
            case_id, 
            disposition_status, 
            disposition_status_date,
            disposition_code, 
            disposition_date, 
            judge
        )
        VALUES (%s, %s, %s, %s, %s, %s)
        RETURNING disposition_id
    """, (
        case_id,
        disp_data.get("disposition_status"), 
        disp_data.get("disposition_status_date"), 
        disp_data.get("disposition_code_text"), 
        disp_data.get("disposition_date"),
        disp_data.get("judge")
    ))
    disp_id = cur.fetchone()[0]
    LOG.info("[checkpoint] disposition inserted with id: %s", disp_id)
    return disp_id

def insert_event(cur, case_id, event_data):
    if not event_data:
        LOG.debug("[debug] no event data found for case_id: %s", case_id)
        return None
    LOG.info("[checkpoint] inserting event for case_id: %s", case_id)
    cur.execute("""
        INSERT INTO events (
            case_id, event_name, event_date, event_start_time, event_end_time, event_judge, event_courtroom, event_result
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING event_id
    """, (
        case_id,
        event_data.get("event_name"),
        event_data.get("event_date"),
        event_data.get("event_start_time"),
        event_data.get("event_end_time"),
        event_data.get("event_judge"),
        event_data.get("event_courtroom"),
        event_data.get("event_result")
    ))
    event_id = cur.fetchone()[0]
    LOG.info("[checkpoint] event inserted with id: %s", event_id)
    return event_id

"""Removes currency symbols and converts value to a string float, or returns None."""
def clean_currency(value):
    if value is None:
        return None
    try:
        # Remove commas, dollar signs, and any leading/trailing whitespace
        cleaned = str(value).strip().replace('$', '').replace(',', '')
        # Check if the remaining string is empty (e.g., if input was just '$')
        if not cleaned:
            return None

        return float(cleaned)
    except ValueError:
        LOG.error(f"Failed to convert non-numeric value: {value}")
        return None


def insert_docket_entries(cur, case_id, docket_list):
    if not docket_list:
        LOG.debug("[debug] no docket entries found for case_id: %s", case_id)
        return 0
    
    DOCKET_CURRENCY = 'USD'
    
    inserted_count = 0
    for docket in docket_list:
        LOG.info("[checkpoint] inserting docket for case_id: %s", case_id)
        
        # 💡 FIX: Apply the cleanup function to amount and balance before execution
        docket_amount = clean_currency(docket.get("docket_amount"))
        docket_balance = clean_currency(docket.get("docket_balance"))
        cur.execute("""
            INSERT INTO docket (
                case_id, docket_date, docket_text, docket_amount, docket_balance, docket_currency
            )
            VALUES (%s, %s, %s, %s, %s, %s)
            RETURNING docket_id
        """, (
            case_id,
            docket.get("docket_date"),
            docket.get("docket_text"),
            docket_amount,
            docket_balance, 
            DOCKET_CURRENCY
        ))
        _ = cur.fetchone()[0]
        inserted_count += 1
    LOG.info("[checkpoint] total docket entries inserted for case_id %s: %d", case_id, inserted_count)
    return inserted_count


