# managed_demo.py -Linear, step-by-step demo for managed MySQL (Azure/GCP/OCI)
# Run this file  top-to-bottom OR run it cell-by-cell in VS Code.
# Prerequisites: 
#   pip install sqlalchemy pymysql pandas python-dotenv
#
# Env vars (populate a local .env):
#   MAN_DB_HOST, MAN_DB_PORT, MAN_DB_USER, MAN_DB_PASS, MAN_DB_NAME

import os
import time
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv


# --- 0) Load env vars
load_dotenv()  # read environment from .env (if present)

# Support multiple possible environment variable naming conventions
# Prefer MAN_DB_* (documented), then AZURE_DB_*, then generic DB_* fallbacks.
DB_HOST = os.getenv("MAN_DB_HOST") or os.getenv("AZURE_DB_HOST") or os.getenv("DB_HOST")
DB_PORT = os.getenv("MAN_DB_PORT") or os.getenv("AZURE_DB_PORT") or os.getenv("DB_PORT") or "3306"
DB_USER = os.getenv("MAN_DB_USER") or os.getenv("AZURE_DB_USER") or os.getenv("DB_USER")
DB_PASS = os.getenv("MAN_DB_PASS") or os.getenv("AZURE_DB_PASS") or os.getenv("DB_PASS")
DB_NAME = os.getenv("MAN_DB_NAME") or os.getenv("AZURE_DB_NAME") or os.getenv("DB_NAME")

print("[ENV] DB_HOST:", DB_HOST)
print("[ENV] DB_PORT:", DB_PORT)
print("[ENV] DB_USER:", DB_USER)
print("[ENV] DB_PASS:", "*****" if DB_PASS else None)
print("[ENV] DB_NAME:", DB_NAME)

# Validate required environment variables early and give a clear message
missing = [name for name, val in (
    ("DB_HOST", DB_HOST),
    ("DB_USER", DB_USER),
    ("DB_PASS", DB_PASS),
    ("DB_NAME", DB_NAME),
) if not val]
if missing:
    raise SystemExit(
        "Missing required environment variables: " + ", ".join(missing)
        + ". Copy your provider .env.example to .env and set values, or set MAN_DB_* / AZURE_DB_* vars."
    )



# --- 1) Connect to server and ensure DB exists
server_url = f"mysql+pymysql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}?ssl=false"
print("[step 1] Connecting to Managed MySQL at:", server_url.replace(DB_PASS or "", "*****"))
t0 = time.time()

engine = create_engine(server_url, pool_pre_ping=True) 
with engine.connect() as conn:
    conn.execute(text(f"CREATE DATABASE IF NOT EXISTS {DB_NAME}"))
    conn.execute(text(f"USE {DB_NAME}"))
    conn.execute(text(f"""
        CREATE TABLE IF NOT EXISTS users (
            id INT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(100),
            email VARCHAR(100)
        )
    """))
    # Ensure radiology_procedures table exists for the demo
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS radiology_procedures (
            procedure_code VARCHAR(20),
            description VARCHAR(255),
            duration_min INT
        )
    """))
    result = conn.execute(text(f"SHOW TABLES"))
    tables = result.fetchall()
    print("[step 1] Tables in database:", tables)    
    
    # --- 2) Connect to the target database and insert data
    db_url = f"mysql+pymysql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}?ssl=false"
    engine = create_engine(db_url, pool_pre_ping=True)
    
    #--3) Create a DataFrame and write to a table ---
    table_name = "radiology_procedures"
    df = pd.DataFrame(
        [
            {"procedure_code": "XR101", "description": "X-Ray chest", "duration_min": 15}
        ]
    )
    print(f"[step 3] Inserting data into table '{table_name}':\n", df)
    # Insert the single-row demo DataFrame
    try:
        df.to_sql(table_name, con=engine, if_exists="append", index=False)
        print(f"[step 3] Inserted {len(df)} rows into '{table_name}'.")
    except Exception as e:
        print(f"[step 3] Warning: could not write to '{table_name}': {e}")
    
    # ---4) Read back a quick check ---
    print(f"[step 4] Reading back data from table '{table_name}':")
    with engine.connect() as conn:
        count_df = pd.read_sql(
            f"SELECT COUNT(*) AS count FROM `{table_name}`", conn
        )
        print(count_df)
        
        elapsed = time.time() - t0
        print(f"[done] Elapsed time: {elapsed:.2f} seconds")
from urllib.parse import quote_plus

_user = quote_plus(DB_USER or "")
_pwd = quote_plus(DB_PASS or "")
server_url = f"mysql+pymysql://{_user}:{_pwd}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(server_url, pool_pre_ping=True)

# --- 5) Bonus: Full demo inserting multiple rows ---
data: dict[str, list[str | int]] = {
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
