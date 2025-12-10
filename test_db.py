import os
from dotenv import load_dotenv
import psycopg2

# Load .env
load_dotenv(dotenv_path="/Users/dishapatel/airflow_scheduling/.env")

# Print to confirm
print("DB_NAME =", os.getenv("DB_NAME"))
print("DB_USER =", os.getenv("DB_USER"))
print("DB_PASSWORD =", os.getenv("DB_PASSWORD"))
print("DB_HOST =", os.getenv("DB_HOST"))
print("DB_PORT =", os.getenv("DB_PORT"))

# Attempt to connect to Postgres
try:
    conn = psycopg2.connect(
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT")
    )
    print("✓ Database connection successful")
    conn.close()
except Exception as e:
    print("✗ Database connection failed:", e)
