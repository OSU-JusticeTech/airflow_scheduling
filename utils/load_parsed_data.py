import logging
import psycopg2
import sys
from .parsers import parse_case_html
from .data_insertion import create_postgres_config
from .gliner_django_loader import load_case_payload

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
    """Parse unprocessed raw_cases rows and insert structured data into Postgres."""
    print(f"\nStarting data parsing with batch size: {batch_size}\n")
    
    config = create_postgres_config()
    conn = psycopg2.connect(**config)
    print(" Database connection established")

    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT r.case_number, r.raw_html
                FROM public.raw_cases r
                WHERE r.raw_html IS NOT NULL
                  AND NOT EXISTS (
                      SELECT 1
                      FROM public.cases c
                      WHERE c.case_number = r.case_number
                  )
                ORDER BY r.case_number
                LIMIT %s
                """,
                (batch_size,)
            )
            rows = cur.fetchall()

            print(f"Found {len(rows)} unparsed records\n")
            LOG.info("[checkpoint] found %d unparsed records", len(rows))

            if len(rows) == 0:
                return

            for idx, (case_number, text) in enumerate(rows, start=1):
                print(f"\nProcessing case {idx}/{len(rows)}: {case_number}")
                LOG.info("[checkpoint] processing case %d/%d: %s", idx, len(rows), case_number)
                parsed = parse_case_html(text)
                
                # Display parsed data for validation
                print(f"\nPARSED DATA for {case_number}:")
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
                    result = load_case_payload(
                        parsed,
                        source_id=case_number,
                        dag_run_id=dag_run_id,
                        task_id=task_id,
                    )
                    case_id = result["case_id"]
                    conn.commit()
                    print(f"Successfully inserted case #{case_id}\n")
                    LOG.info("[checkpoint] finished processing case: %s", case_number)

                except Exception as e:
                    conn.rollback()
                    print(f"Error processing {case_number}: {e}\n")
                    LOG.error("[error] failed to process case %s: %s", case_number, e, exc_info=True)
    finally:
        conn.close()
        print("Database connection closed")
        LOG.info("[checkpoint] connection closed after batch processing")

if __name__ == "__main__":
    parse_and_insert_from_db(batch_size=1, dag_run_id='local_test_run_2025', 
        task_id='local_insert_task')
