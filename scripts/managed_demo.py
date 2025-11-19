"""
managed_demo.py
Step-by-step demo for a managed MySQL instance with an optional local SQLite fallback.

Usage:
  python managed_demo.py --env azure           # uses Azure MySQL
  python managed_demo.py --env vm              # uses VM MySQL
  python managed_demo.py --env gcp             # uses GCP MySQL
  python managed_demo.py --sqlite              # forces local SQLite fallback

Env vars supported (in order of precedence):
  MAN_DB_*  (preferred)
  AZURE_DB_*
  DB_* (generic)
"""

import os
import time
import argparse
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from typing import Dict, List, Union

# --- Parse arguments ---
parser = argparse.ArgumentParser(description="Managed demo with optional SQLite fallback")
parser.add_argument("--env", choices=["azure", "vm", "gcp"], default="azure", help="Managed environment to target")
parser.add_argument("--sqlite", action="store_true", help="Force use of local SQLite instead of managed MySQL")
args = parser.parse_args()

print(f"[MODE] Running in {args.env} mode")
if args.sqlite:
    print("[MODE] --sqlite given: forcing SQLite fallback")
elif args.env == "azure":
    print("[MODE] Using Azure Managed MySQL")
elif args.env == "vm":
    print("[MODE] Using VM MySQL")
elif args.env == "gcp":
    print("[MODE] Using GCP Managed MySQL")

# --- Load env vars ---
load_dotenv()

DB_HOST = os.getenv("MAN_DB_HOST") or os.getenv("AZURE_DB_HOST") or os.getenv("DB_HOST")
DB_PORT_RAW = os.getenv("MAN_DB_PORT") or os.getenv("AZURE_DB_PORT") or os.getenv("DB_PORT") or "3306"
DB_USER = os.getenv("MAN_DB_USER") or os.getenv("AZURE_DB_USER") or os.getenv("DB_USER")
DB_PASS = os.getenv("MAN_DB_PASS") or os.getenv("AZURE_DB_PASS") or os.getenv("DB_PASS")
DB_NAME = os.getenv("MAN_DB_NAME") or os.getenv("AZURE_DB_NAME") or os.getenv("DB_NAME")

try:
    DB_PORT = int(DB_PORT_RAW)
except ValueError:
    raise ValueError(f"[ERROR] Invalid DB_PORT value: {DB_PORT_RAW}")

print("[ENV] DB_HOST:", DB_HOST)
print("[ENV] DB_PORT:", DB_PORT)
print("[ENV] DB_USER:", DB_USER)
print("[ENV] DB_PASS:", "*****" if DB_PASS else None)
print("[ENV] DB_NAME:", DB_NAME)

missing = [name for name, val in (("DB_HOST", DB_HOST), ("DB_USER", DB_USER), ("DB_PASS", DB_PASS), ("DB_NAME", DB_NAME)) if not val]
use_sqlite_fallback = args.sqlite or bool(missing)

# --- Connect to DB ---
t0 = time.time()

if not use_sqlite_fallback:
    server_url = f"mysql+pymysql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    connect_args = {"ssl_disabled": True}
    print("[DEBUG] Connection string:", server_url.replace(DB_PASS or "", "*****"))
    engine = create_engine(server_url, pool_pre_ping=True, connect_args=connect_args)
    with engine.connect() as conn:
        conn.execute(text(f"CREATE DATABASE IF NOT EXISTS {DB_NAME}"))
        conn.execute(text(f"USE {DB_NAME}"))
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS users (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(100),
                email VARCHAR(100)
            )
        """))
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS radiology_procedures (
                procedure_code VARCHAR(20),
                description VARCHAR(255),
                duration_min INT
            )
        """))
        result = conn.execute(text("SHOW TABLES"))
        tables = result.fetchall()
        print("[step 1] Tables in database:", tables)
else:
    print("[step 1] Using local SQLite database 'managed_demo_local.db'")
    engine = create_engine("sqlite:///managed_demo_local.db")
    with engine.connect() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT,
                email TEXT
            )
        """))
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS radiology_procedures (
                procedure_code TEXT,
                description TEXT,
                duration_min INTEGER
            )
        """))

# --- Insert demo row ---
table_name = "radiology_procedures"
df = pd.DataFrame([
    {"procedure_code": "XR101", "description": "X-Ray chest", "duration_min": 15}
])
print(f"[step 3] Inserting data into table '{table_name}':\n", df)
try:
    df.to_sql(table_name, con=engine, if_exists="append", index=False)
    print(f"[step 3] Inserted {len(df)} rows into '{table_name}'.")
except Exception as e:
    print(f"[step 3] Warning: could not write to '{table_name}': {e}")

# --- Read back ---
print(f"[step 4] Reading back data from table '{table_name}':")
with engine.connect() as conn:
    count_df = pd.read_sql(f"SELECT COUNT(*) AS count FROM {table_name}", conn) # type: ignore
    print(count_df)
    elapsed = time.time() - t0
    print(f"[done] Elapsed time: {elapsed:.2f} seconds")

# --- Insert multiple rows ---
data: Dict[str, List[Union[str, int]]] = {
    "procedure_code": ["XR101", "XR102", "XR103"],
    "description": ["X-Ray chest", "X-Ray abdomen", "X-Ray pelvis"],
    "duration_min": [15, 20, 25]
}
df = pd.DataFrame(data)
print(f"[step 5] Inserting multiple rows into table '{table_name}':\n", df)
try:
    df.to_sql(table_name, con=engine, if_exists="append", index=False)
    print(f"[step 5] Inserted {len(df)} rows into '{table_name}'.")
except Exception as e:
    print(f"[step 5] Warning: could not write multiple rows to '{table_name}': {e}")

# --- Final preview ---
print(f"[step 6] Reading back data from table '{table_name}':")
with engine.connect() as conn:
    preview_df = pd.read_sql(f"SELECT * FROM {table_name} LIMIT 5", conn) # type: ignore
    print(preview_df)

