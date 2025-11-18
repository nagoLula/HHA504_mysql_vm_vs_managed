# db_demo.py
import argparse
import pandas as pd
from pandas import DataFrame
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine, Connection
from dotenv import load_dotenv
from urllib.parse import quote_plus
import os

# --- 1. Load environment variables ---
load_dotenv(".env")

# --- 2. Parse command-line arguments ---
parser = argparse.ArgumentParser(description="Run demo against VM or managed MySQL")
parser.add_argument("--env", choices=["vm", "azure", "gcp"], default="vm", help="Which DB environment to use")
args = parser.parse_args()

# --- 3. Extract environment-specific variables ---
prefix = args.env.upper() + "_DB_"
DB_HOST = os.getenv(f"{prefix}HOST")
DB_PORT = os.getenv(f"{prefix}PORT") or "3306"
DB_USER = os.getenv(f"{prefix}USER")
DB_PASS = os.getenv(f"{prefix}PASS")
DB_NAME = os.getenv(f"{prefix}NAME")

print(f"[ENV] Using {args.env.upper()} config:")
print(f"  Host: {DB_HOST}")
print(f"  Port: {DB_PORT}")
print(f"  User: {DB_USER}")
print(f"  DB:   {DB_NAME}")

# --- 4. Create SQLAlchemy engine for the initial demo block ---
# If the VM env vars are missing, fall back to a local SQLite database so the
# demo can run end-to-end without an external MySQL server.
top_missing = [name for name, val in (("DB_HOST", DB_HOST), ("DB_USER", DB_USER), ("DB_PASS", DB_PASS), ("DB_NAME", DB_NAME)) if not val]
use_top_sqlite = False
if top_missing:
    print("[INFO] VM credentials missing for initial demo block: " + ", ".join(top_missing) + ". Using local SQLite for initial demo.")
    engine = create_engine("sqlite:///vm_demo_top.db")
    use_top_sqlite = True
else:
    server_url = f"mysql+pymysql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    engine = create_engine(server_url, pool_pre_ping=True)

# --- 5. Create table and insert data (initial demo block) ---
with engine.connect() as raw_conn:
    conn: Connection = raw_conn
    # For MySQL we attempted to create and select the DB above; for SQLite skip CREATE DATABASE/USE
    if not use_top_sqlite:
        conn.execute(text(f"CREATE DATABASE IF NOT EXISTS {DB_NAME}"))
        conn.execute(text(f"USE {DB_NAME}"))

    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS procedures (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            procedure_code VARCHAR(10),
            description VARCHAR(100),
            duration_min INT
        )
    """))


with engine.connect() as conn:  # type: ignore[assignment]
    # Use a typed Connection and pass it as `con=` to help the type checker
    # and avoid ambiguous overload resolution for pd.read_sql.
    count_df: DataFrame = pd.read_sql("SELECT COUNT(*) AS count FROM procedures", con=conn) # type: ignore
    print("[Query] Row count:", int(count_df.iloc[0]["count"]))

    preview_df: DataFrame = pd.read_sql("SELECT * FROM procedures LIMIT 1", con=conn) # type: ignore
    # preview_df now contains the single-row preview

# Load enviroment variables
load_dotenv()  # type: ignore

# VM credencials
host = os.getenv("VM_DB_HOST")
port = os.getenv("VM_DB_PORT") or "3306"
user = os.getenv("VM_DB_USER")
password = os.getenv("VM_DB_PASS")
db_name = os.getenv("VM_DB_NAME")

# Validate required environment variables early and give a clear message
missing = [name for name, val in (
    ("VM_DB_HOST", host),
    ("VM_DB_USER", user),
    ("VM_DB_PASS", password),
    ("VM_DB_NAME", db_name),
) if not val]
use_sqlite_fallback = False
if missing:
    print(
        "Missing DB env vars: " + ", ".join(missing)
        + ". Falling back to a local SQLite DB for demo purposes."
    )
    use_sqlite_fallback = True

# Connection string
# URL-encode credentials to safely handle special characters in user/password
engine_vm: Engine
if use_sqlite_fallback:
    engine_vm = create_engine("sqlite:///vm_demo_local.db")
else:
    # URL-encode credentials to safely handle special characters in user/password
    _user = quote_plus(user or "")
    _pwd = quote_plus(password or "")
    _db = db_name or ""
    # include charset and ensure port has a default
    engine_vm = create_engine(f"mysql+pymysql://{_user}:{_pwd}@{host}:{port}/{_db}?charset=utf8mb4")
with engine_vm.connect() as raw_conn:  # type: ignore
    conn: Connection = raw_conn
    # For MySQL: ensure database exists and is selected
    if not use_sqlite_fallback:
        # safe-ish for demo; db_name was validated above
        conn.execute(text(f"CREATE DATABASE IF NOT EXISTS {db_name}"))
        conn.execute(text(f"USE {db_name}"))

    # Create table if not exists (works for both SQLite and MySQL)
    conn.execute(
        text("""
        CREATE TABLE IF NOT EXISTS radiology_procedures (
            procedure_code TEXT,
            procedure_name TEXT,
            duration_min INTEGER
        )
        """)
    )

# Radiology_procedures data (single source of truth)
data: dict[str, list[str | int]] = {
    "procedure_code": ["XR101", "CT202", "MRI303", "US404", "XR505"],
    "procedure_name": ["X-Ray Spine", "CT Chest", "MRI Knee", "Ultrasound Liver", "X-Ray Hand"],
    "duration_min": [12, 28, 50, 18, 10]
}

# from sqlalchemy.engine import Engine  # Removed duplicate/unnecessary import

df = pd.DataFrame(data)
df = pd.DataFrame(data)
# Use to_sql to (re)populate the table
df.to_sql("radiology_procedures", con=engine_vm, if_exists="replace", index=False)
with engine_vm.connect() as conn:
    cursor = conn.execute(text("SELECT * FROM radiology_procedures"))
    rows = cursor.mappings().all()
    columns = list(cursor.keys())
    result: DataFrame = pd.DataFrame(rows, columns=columns)
print(result)
print(result)
print(f"Row count: {len(result)}")
print("[Schema check] Columns:", result.columns.tolist())
first_row: dict[str, object] = result.iloc[0].to_dict()  # type: ignore
print("[Preview] First row:", first_row)
print("[DEBUG] VM_DB_HOST:", os.getenv("VM_DB_HOST"))
print("[DEBUG] DB_HOST:", DB_HOST)


# Debug info removed to avoid importing sibling modules at runtime

# Load environment variables from .env file
# Connect to VM-hosted MySQL using SQLAlchemy
# Create database and table if not exists
# Insert radiology procedures data
# Read back and print results