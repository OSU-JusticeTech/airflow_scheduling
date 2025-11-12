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

def insert_case(cur, parsed):
    LOG.info("[checkpoint] inserting case: %s", parsed.get("case_number"))
    cur.execute("""
        INSERT INTO cases (
            case_number, case_title, case_description, case_status, case_filed_date
        )
        VALUES (%s, %s, %s, %s, %s)
        RETURNING case_id
    """, (
        parsed.get("case_number"),
        parsed.get("case_title"),
        parsed.get("case_description"),
        parsed.get("case_status"),
        parsed.get("case_filed_date")
    ))
    case_id = cur.fetchone()[0]
    LOG.info("[checkpoint] case inserted with id: %s", case_id)
    return case_id

def insert_address(cur, addr_dict, addr_type=None):
    if not addr_dict:
        return None
    cur.execute("""
        INSERT INTO address (
            address_type, address_line1, address_line2, city, state, postal_code, country
        ) VALUES (%s, %s, %s, %s, %s, %s, %s)
        RETURNING address_id
    """, (
        addr_type,
        addr_dict.get("address_line1"),
        addr_dict.get("address_line2"),
        addr_dict.get("city"),
        addr_dict.get("state"),
        addr_dict.get("postal_code"),
        addr_dict.get("country", "USA")
    ))
    return cur.fetchone()[0]

def insert_party(cur, case_id, party_data):
    addr_id = insert_address(cur, party_data, addr_type="party")
    LOG.info("[checkpoint] inserting party for case_id: %s", case_id)
    cur.execute("""
        INSERT INTO party (party_type, address_id)
        VALUES (%s, %s)
        RETURNING party_id
    """, (party_data.get("type"), addr_id))
    party_id = cur.fetchone()[0]

    cur.execute("INSERT INTO case_party (case_id, party_id) VALUES (%s, %s)", (case_id, party_id))
    LOG.info("[checkpoint] party inserted with id: %s", party_id)
    return party_id

def insert_attorney(cur, case_id, atty_data):
    if not atty_data:
        LOG.debug("[debug] no attorney data found for case_id: %s", case_id)
        return None
    addr_id = insert_address(cur, atty_data, addr_type="attorney")
    LOG.info("[checkpoint] inserting attorney party for case_id: %s", case_id)
    cur.execute("""
        INSERT INTO party (party_type, address_id)
        VALUES (%s, %s)
        RETURNING party_id
    """, (atty_data.get("party_type"), addr_id))
    party_id = cur.fetchone()[0]

    LOG.info("[checkpoint] inserting attorney details for party_id: %s", party_id)
    cur.execute("""
        INSERT INTO attorney (party_id, attorney_type, address_id)
        VALUES (%s, %s, %s)
        RETURNING attorney_id
    """, (party_id, atty_data.get("party_type"), addr_id))
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
            case_id, disposition_status, disposition_status_date,
            disposition_code, disposition_date, judge
        )
        VALUES (%s, %s, %s, %s, %s, %s)
        RETURNING disposition_id
    """, (
        case_id,
        disp_data.get("status"),
        disp_data.get("status_date"),
        disp_data.get("disposition_code"),
        disp_data.get("disposition_date"),
        disp_data.get("judge")
    ))
    disp_id = cur.fetchone()[0]
    LOG.info("[checkpoint] disposition inserted with id: %s", disp_id)
    return disp_id

def insert_docket_entries(cur, case_id, docket_list):
    if not docket_list:
        LOG.debug("[debug] no docket entries found for case_id: %s", case_id)
        return 0
    inserted_count = 0
    for docket in docket_list:
        LOG.info("[checkpoint] inserting docket for case_id: %s", case_id)
        cur.execute("""
            INSERT INTO docket (
                case_id, docket_date, docket_text, docket_amount, docket_balance
            )
            VALUES (%s, %s, %s, %s, %s)
            RETURNING docket_id
        """, (
            case_id,
            docket.get("docket_date"),
            docket.get("docket_text"),
            docket.get("docket_amount"),
            docket.get("docket_balance")
        ))
        _ = cur.fetchone()[0]
        inserted_count += 1
    LOG.info("[checkpoint] total docket entries inserted for case_id %s: %d", case_id, inserted_count)
    return inserted_count
