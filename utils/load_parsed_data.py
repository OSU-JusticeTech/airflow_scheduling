import logging
import psycopg2
import sys
from .parsers import parse_case_html
from .data_insertion import (
    create_postgres_config,
    insert_case,
    insert_party,
    insert_attorney,
    insert_disposition,
    insert_docket_entries, 
    update_pipeline_status_to_valid,
    update_pipeline_status_single, 
    insert_event,
)

LOG = logging.getLogger(__name__)
LOG.setLevel(logging.INFO)

# Add console handler to output logs to stdout
if not LOG.handlers:
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(formatter)
    LOG.addHandler(console_handler)

def parse_and_insert_from_db(batch_size=5, dag_run_id=None, task_id=None):
    """Parse unprocessed raw_case_data and insert structured data into Postgres."""
    print(f"\nStarting data parsing with batch size: {batch_size}\n")
    
    config = create_postgres_config()
    conn = psycopg2.connect(**config)
    print("✓ Database connection established")

    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT file_name, extracted_text
                FROM public.raw_case_data
                WHERE parsed IS DISTINCT FROM TRUE
                LIMIT %s
                """,
                (batch_size,)
            )
            rows = cur.fetchall()

            print(f"Found {len(rows)} unparsed records\n")
            LOG.info("[checkpoint] found %d unparsed records", len(rows))

            if len(rows) == 0:
                return

            for idx, (file_name, text) in enumerate(rows, start=1):
                print(f"\nProcessing file {idx}/{len(rows)}: {file_name}")
                LOG.info("[checkpoint] processing file %d/%d: %s", idx, len(rows), file_name)
                parsed = parse_case_html(text)
                
                # Display parsed data for validation
                print(f"\nPARSED DATA for {file_name}:")
                print(f"   Case Number: {parsed.get('case_number')}")
                print(f"   Case Title: {parsed.get('case_title')}")
                print(f"   Case Status: {parsed.get('case_status')}")
                print(f"   Case Filed Date: {parsed.get('case_filed_date')}")
                print(f"   Parties ({len(parsed.get('parties', []))}): {parsed.get('parties', [])}")
                print(f"   Attorneys ({len(parsed.get('attorneys', []))}): {parsed.get('attorneys', [])}")
                print(f"   Disposition: {parsed.get('disposition')}")
                print(f"   Events ({len(parsed.get('events', []))}): {parsed.get('events', [])}")
                print(f"   Dockets ({len(parsed.get('dockets', []))}): {parsed.get('dockets', [])}")
                

                try:
                    # insert everything using the same cursor
                    case_id = insert_case(cur, parsed, dag_run_id)
                    for p in parsed.get("parties", []):
                        insert_party(cur, case_id, p)
                    for a in parsed.get("attorneys", []):
                        insert_attorney(cur, case_id, a)
                    insert_disposition(cur, case_id, parsed.get("disposition"))
                    for e in parsed.get("events", []):
                        insert_event(cur, case_id, e)
                    insert_docket_entries(cur, case_id, parsed.get("dockets", []))
                    
                    # update pipeline status to 'valid' in CASES after successful insertion
                    update_pipeline_status_single(cur, case_id, 'Valid', task_id)

                    # mark raw data as parsed
                    cur.execute(
                        "UPDATE public.raw_case_data SET parsed = TRUE WHERE file_name=%s",
                        (file_name,)
                    )
                    conn.commit()
                    print(f"Successfully inserted case #{case_id}\n")
                    LOG.info("[checkpoint] finished processing file: %s", file_name)

                except Exception as e:
                    conn.rollback()
                    print(f"Error processing {file_name}: {e}\n")
                    LOG.error("[error] failed to process file %s: %s", file_name, e, exc_info=True)
    finally:
        conn.close()
        print("✓ Database connection closed")
        LOG.info("[checkpoint] connection closed after batch processing")

if __name__ == "__main__":
    parse_and_insert_from_db(batch_size=1, dag_run_id='local_test_run_2025', 
        task_id='local_insert_task')
