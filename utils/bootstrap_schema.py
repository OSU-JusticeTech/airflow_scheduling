import psycopg2

from config.runtime import DB_CONFIG


DDL_STATEMENTS = [
    """
    CREATE TABLE IF NOT EXISTS raw_cases (
        case_id BIGSERIAL PRIMARY KEY,
        case_number TEXT UNIQUE,
        case_title TEXT,
        case_description TEXT,
        case_status TEXT,
        case_filed_date DATE,
        created TIMESTAMP,
        created_by TEXT,
        updated TIMESTAMP,
        updated_by TEXT,
        first_party_name TEXT,
        first_party_type TEXT,
        first_attorney_name TEXT,
        first_attorney_type TEXT,
        first_event_start TIMESTAMP,
        first_event_end TIMESTAMP,
        first_event_judge TEXT,
        parties JSONB,
        attorneys JSONB,
        events JSONB,
        docket JSONB,
        dispositions JSONB,
        raw_html TEXT,
        ingested_at TIMESTAMP
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS cases (
        case_id BIGSERIAL PRIMARY KEY,
        case_number TEXT UNIQUE,
        case_title TEXT,
        case_description TEXT,
        case_status TEXT,
        case_filed_date DATE,
        created TIMESTAMP DEFAULT NOW(),
        created_dag_run_id TEXT,
        updated TIMESTAMP DEFAULT NOW(),
        updated_task_id TEXT,
        pipeline_status TEXT DEFAULT 'New' NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS address (
        address_id BIGSERIAL PRIMARY KEY,
        address_type TEXT,
        address_line1 TEXT,
        address_line2 TEXT,
        city TEXT,
        state TEXT,
        country TEXT,
        postal_code TEXT,
        created_at TIMESTAMP DEFAULT NOW()
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS party (
        party_id BIGSERIAL PRIMARY KEY,
        party_name TEXT,
        party_type TEXT,
        address_id BIGINT REFERENCES address(address_id),
        created TIMESTAMP DEFAULT NOW(),
        created_dag_run_id TEXT,
        updated TIMESTAMP,
        updated_task_id TEXT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS case_party (
        case_id BIGINT NOT NULL REFERENCES cases(case_id),
        party_id BIGINT NOT NULL REFERENCES party(party_id),
        PRIMARY KEY (case_id, party_id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS attorney (
        attorney_id BIGSERIAL PRIMARY KEY,
        attorney_name TEXT,
        party_id BIGINT REFERENCES party(party_id),
        attorney_type TEXT,
        address_id BIGINT REFERENCES address(address_id),
        created TIMESTAMP DEFAULT NOW(),
        created_dag_run_id TEXT,
        updated TIMESTAMP,
        updated_task_id TEXT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS disposition (
        disposition_id BIGSERIAL PRIMARY KEY,
        case_id BIGINT REFERENCES cases(case_id),
        disposition_status TEXT,
        disposition_status_date DATE,
        disposition_code TEXT,
        disposition_date DATE,
        judge TEXT,
        created TIMESTAMP,
        created_dag_run_id TEXT,
        updated TIMESTAMP,
        updated_task_id TEXT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS events (
        event_id BIGSERIAL PRIMARY KEY,
        case_id BIGINT REFERENCES cases(case_id),
        event_name TEXT,
        event_date DATE,
        event_start_time TEXT,
        event_end_time TEXT,
        event_judge TEXT,
        event_courtroom TEXT,
        event_result TEXT,
        created TIMESTAMP,
        created_dag_run_id TEXT,
        updated TIMESTAMP,
        updated_task_id TEXT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS docket (
        docket_id BIGSERIAL PRIMARY KEY,
        case_id BIGINT REFERENCES cases(case_id),
        docket_date DATE,
        docket_text TEXT,
        docket_currency TEXT,
        docket_amount NUMERIC(20, 2),
        docket_balance NUMERIC(20, 2),
        created TIMESTAMP,
        created_dag_run_id TEXT,
        updated TIMESTAMP,
        updated_task_id TEXT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS geocoded_addresses (
        geocoded_id BIGSERIAL PRIMARY KEY,
        address_id BIGINT UNIQUE REFERENCES address(address_id),
        case_id BIGINT REFERENCES cases(case_id),
        latitude DOUBLE PRECISION,
        longitude DOUBLE PRECISION,
        geocode_status TEXT,
        geocoded_at TIMESTAMP,
        geocode_service TEXT,
        created_at TIMESTAMP DEFAULT NOW()
    )
    """,
]


def main() -> None:
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        with conn.cursor() as cur:
            for ddl in DDL_STATEMENTS:
                cur.execute(ddl)
        conn.commit()
        print("Application schema bootstrap complete.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
