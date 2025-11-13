# Airflow Case Data ETL Pipeline

This project uses Apache Airflow to orchestrate an ETL (Extract, Transform, Load) pipeline for processing legal case data. The pipeline reads HTML files, parses case information, and loads the structured data into a PostgreSQL database.

## Project Structure

```
airflow/
├── dags/                          # Airflow DAG definitions
│   └── case_data_etl.py          # Main ETL DAG
├── db_info/                      # database information
│   └── schemas, diagrams, xslx    # diagrams and setup init database info
├── utils/                         # Utility modules for parsing and data insertion
│   ├── parsers.py                # HTML parsing functions
│   ├── parser_updated.py         # Updated parser with additional logic
│   ├── data_insertion.py         # Database insertion helper functions
│   ├── load_parsed_data.py       # Load parsed data into structured tables
│   └── db_check.py               # Database utility checks
├── logs/                          # Airflow execution logs
├── plugins/                       # Custom Airflow plugins (if any)
├── airflow.cfg                   # Airflow configuration file
├── requirements.txt              # Python dependencies
└── README.md                    # This file
```

## Prerequisites

- Python 3.8 or higher
- PostgreSQL 12 or higher
- pip (Python package manager)

## Setup Instructions

### 1. Clone the Repository

```bash
git clone https://github.com/OSU-JusticeTech/airflow_scheduling.git
cd airflow_scheduling
```

### 2. Create a Python Virtual Environment

```bash
python3 -m venv venv
source venv/bin/activate  # On macOS/Linux
# or
venv\Scripts\activate  # On Windows
```

### 3. Install Dependencies

```bash
pip install -r requirements.txt
```

The `requirements.txt` includes:
- Apache Airflow 2.7.1
- PostgreSQL adapter (psycopg2)
- BeautifulSoup4 (for HTML parsing)
- Flask and related web framework packages
- And other supporting libraries

### 4. Configure Environment Variables

Create a `.env` file in the project root with the following variables:

```env
DB_USER=your_postgres_user
DB_PASSWORD=your_postgres_password
DB_HOST=localhost
DB_PORT=5432
DB_NAME=justicetech_db
```

These variables are used by the Airflow DAGs to connect to PostgreSQL.

## Database Setup

### 1. Create PostgreSQL Database

```bash
# Connect to PostgreSQL
psql -U postgres

# Create database
CREATE DATABASE justicetech_db;

# Connect to the new database
\c justicetech_db

# Create the airflow_user (optional, for better security)
CREATE USER airflow_user WITH PASSWORD 'your_secure_password';
GRANT ALL PRIVILEGES ON DATABASE justicetech_db TO airflow_user;
```

#### if all else fails...use postgres (main user, which is what currently what is being used on Yash's local)


### 2. Create Database Tables

Run the SQL schema to create necessary tables. Execute following SQL commands from folder db_info in the format shown below:

```sql
-- Raw case data table (stores extracted HTML text)
CREATE TABLE IF NOT EXISTS public.raw_case_data (
    id SERIAL PRIMARY KEY,
    file_name VARCHAR(255) UNIQUE,
    extracted_text TEXT,
    parsed BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT NOW()
);
```

### 3. Verify Database Connection

Test the connection from your application:

```bash
python -c "
import psycopg2
from dotenv import load_dotenv
import os

load_dotenv()
try:
    conn = psycopg2.connect(
        dbname=os.getenv('DB_NAME'),
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD'),
        host=os.getenv('DB_HOST'),
        port=os.getenv('DB_PORT')
    )
    print('✓ Database connection successful')
    conn.close()
except Exception as e:
    print(f'✗ Connection failed: {e}')
"
```

## Running Airflow

### 1. Initialize Airflow Database

```bash
# This creates the Airflow metadata database
airflow db init
```

### 2. Create an Airflow Admin User

```bash
airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com
    --password admin  # Change this to a secure password (for you)
```

### 3. Start the Airflow Webserver

```bash
# Start the web UI on http://localhost:8080
airflow webserver -p 8080
```

### 4. Start the Airflow Scheduler (in a separate terminal)

```bash
# Activate your virtual environment first
source venv/bin/activate

# Start the scheduler
airflow scheduler
```

### 5. Access the Airflow UI

Open your web browser and navigate to:
```
http://localhost:8080
```

Log in with your admin credentials and you should see the DAGs listed, including `batch_cases_to_postgres` and `denormalize_cases_dag`.

## Parsing and Data Generation

The project includes utilities to parse HTML files and generate structured data. Here's how to use them:

### 1. Understanding the Parser Files

- **`utils/parsers.py`** - Contains regex-based extraction functions for different data types:
  - `extract_parties_from_text()` - Extracts case parties/litigants
  - `extract_attorneys_from_text()` - Extracts attorney information
  - `extract_disposition_from_text()` - Extracts case disposition/outcome
  - `extract_dockets_from_text()` - Extracts docket entries

- **`utils/parser_updated.py`** - Enhanced parser with additional logic and database operations

- **`utils/data_insertion.py`** - Helper functions to insert parsed data into PostgreSQL:
  - `insert_case()` - Insert case record
  - `insert_address()` - Insert address
  - `insert_party()` - Insert party record
  - `insert_attorney()` - Insert attorney record
  - `insert_disposition()` - Insert disposition
  - `insert_docket_entries()` - Insert docket entries

### 2. Generate Individual Parser Files (still in production for loading data to db)

```bash
# Parse and insert data from raw_case_data table
python -m utils.load_parsed_data

# Or with custom batch size
python -c "
from utils.load_parsed_data import parse_and_insert_from_db
parse_and_insert_from_db(batch_size=10)
"
```


### 4. Processing Large HTML Files

The DAG `batch_cases_to_postgres` in `dags/case_data_etl.py` automatically:
1. Reads HTML files from `~/Desktop/justicetech/cases`
2. Extracts text using BeautifulSoup
3. Inserts raw data into `raw_case_data` table
4. Processes in batches of 1000 files

To trigger the DAG manually:

```bash
# In the Airflow UI, navigate to DAGs and click the play button, or use CLI:
airflow dags trigger batch_cases_to_postgres
```

## Troubleshooting

### Issue: "Database connection refused"

**Solution:**
```bash
# Check PostgreSQL is running
psql -U postgres -h localhost -c "SELECT 1"

# Verify .env file has correct credentials
cat .env
```

### Issue: "Airflow cannot find DAGs"

**Solution:**
```bash
# Check dags_folder path in airflow.cfg
grep "dags_folder" airflow.cfg

# Ensure AIRFLOW_HOME is set correctly
export AIRFLOW_HOME=/Users/yasashwininapa/airflow
```

### Issue: "Permission denied" for airflow_user

**Solution:**
```sql
-- In PostgreSQL, grant proper permissions:
GRANT USAGE ON SCHEMA public TO airflow_user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO airflow_user;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO airflow_user;
```