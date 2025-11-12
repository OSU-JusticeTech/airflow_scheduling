import logging
import psycopg2
from .parsers import parse_case_html
from .data_insertion import (
    create_postgres_config,
    insert_case,
    insert_party,
    insert_attorney,
    insert_disposition,
    insert_docket_entries
)

LOG = logging.getLogger(__name__)
LOG.setLevel(logging.INFO)

def parse_and_insert_from_db(batch_size=5):
    """Parse unprocessed raw_case_data and insert structured data into Postgres."""
    config = create_postgres_config()
    conn = psycopg2.connect(**config)

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

            LOG.info("[checkpoint] found %d unparsed records", len(rows))

            for idx, (file_name, text) in enumerate(rows, start=1):
                LOG.info("[checkpoint] processing file %d/%d: %s", idx, len(rows), file_name)
                parsed = parse_case_html(text)

                try:
                    # insert everything using the same cursor
                    case_id = insert_case(cur, parsed)
                    for p in parsed.get("parties", []):
                        insert_party(cur, case_id, p)
                    for a in parsed.get("attorneys", []):
                        insert_attorney(cur, case_id, a)
                    insert_disposition(cur, case_id, parsed.get("disposition"))
                    insert_docket_entries(cur, case_id, parsed.get("dockets", []))

                    # mark raw data as parsed
                    cur.execute(
                        "UPDATE public.raw_case_data SET parsed = TRUE WHERE file_name=%s",
                        (file_name,)
                    )
                    conn.commit()
                    LOG.info("[checkpoint] finished processing file: %s", file_name)

                except Exception as e:
                    conn.rollback()
                    LOG.error("[error] failed to process file %s: %s", file_name, e, exc_info=True)
    finally:
        conn.close()
        LOG.info("[checkpoint] connection closed after batch processing")

if __name__ == "__main__":
    parse_and_insert_from_db(batch_size=3)
