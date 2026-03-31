import os
from logging.config import fileConfig
from urllib.parse import quote_plus

from alembic import context
from sqlalchemy import create_engine, pool
from dotenv import load_dotenv

# Alembic Config object
config = context.config

# Logging config
fileConfig(config.config_file_name)

# Load .env
load_dotenv(dotenv_path="/Users/dishapatel/airflow_scheduling/.env")

DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")

# Debug print to verify environment variables
print(f"DB_USER: {DB_USER}")
print(f"DB_PASSWORD: {DB_PASSWORD}")
print(f"DB_HOST: {DB_HOST}")
print(f"DB_PORT: {DB_PORT}")
print(f"DB_NAME: {DB_NAME}")

# URL-encode the password to handle special characters like @
encoded_password = quote_plus(DB_PASSWORD) if DB_PASSWORD else ""

DATABASE_URL = (
    f"postgresql+psycopg2://{DB_USER}:{encoded_password}"
    f"@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)
print(f"DATABASE_URL: {DATABASE_URL}")

# Import Base + all models
from models.base import Base
from models import *

target_metadata = Base.metadata


# --- DB connection used by Alembic ---
def run_migrations_offline():
    context.configure(
        url=DATABASE_URL,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online():
    engine = create_engine(
        DATABASE_URL,
        poolclass=pool.NullPool
    )

    with engine.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            compare_type=True,
            compare_server_default=True
        )

        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
