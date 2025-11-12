import psycopg2
from load_parse_data import create_postgres_config

config = create_postgres_config()
conn = psycopg2.connect(**config)

tables = ["cases", "address", "party", "attorney", "disposition", "docket", "case_party"]

with conn.cursor() as cur:
    for t in tables:
        try:
            cur.execute(f"SELECT COUNT(*) FROM {t}")
            print(f"{t} accessible, rows: {cur.fetchone()[0]}")
        except Exception as e:
            print(f"Error accessing {t}: {e}")
