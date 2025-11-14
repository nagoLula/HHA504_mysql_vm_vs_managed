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
load_dotenv(".env") # reads .env file in current dir

DB_HOST = os.getenv("MAN_DB_HOST")
DB_PORT = os.getenv("MAN_DB_PORT")
DB_USER = os.getenv("MAN_DB_USER")
DB_PASS = os.getenv("MAN_DB_PASS")
DB_NAME = os.getenv("MAN_DB_NAME")

print("[ENV] MAN_DB_HOST:", DB_HOST )
print("[ENV] MAN_DB_PORT:", DB_PORT)
print("[ENV] MAN_DB_USER:", DB_USER)
print("[ENV] MAN_DB_PASS:", DB_PASS)
print("[ENV] MAN_DB_NAME:", DB_NAME)

# --- 1) Connect to server and ensure DB exists
server_url =  # type: ignore
f"mysql+pymysql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}?ssl=false"
print("[step 1] Connecting to Managed MySQL at:", server_url.replace(DB_PASS, "*****")) # type: ignore
t0 = time.time()

engine = create_engine(server_url, pool_pre_ping=True) # type: ignore
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
    conn.execute(text(f"SHOW TABLES"))
    result = conn.fetchall() # type: ignore
    print("[step 1] Tables in database:", result)    # type: ignore
    
    # --- 2) Connect to the target database and insert data
    db_url = f"mysql+pymysql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}?ssl=false"
    engine = create_engine(db_url, pool_pre_ping=True) # type: ignore
    
    #--3) Create a DataFrame and write to a table ---
    table_name = "radiology procedures"
    df = pd.DataFrame( # type: ignore
        [
            {"procedure_code": "XR101", "description": "X-Ray chest", "duration_min": 15}
        ]
    )
    print(f"[step 3] Inserting data into table '{table_name}':\n", df) # type: ignore
    
    # ---4) Read back a quick check ---
    print(f"[step 4] Reading back data from table '{table_name}':")
    with engine.connect() as conn:
        count_df = pd.read_sql(f"SELECT COUNT(*) AS count FROM `{table_name}`", conn) # type: ignore
        print(count_df) # type: ignore
        
        elapsed = time.time() - t0
        print(f"[done] Elapsed time: {elapsed:.2f} seconds")
f"mysql+pymysql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/?ssl=false"
