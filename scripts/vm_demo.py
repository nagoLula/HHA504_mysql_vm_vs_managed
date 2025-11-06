import os    
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
from urllib.parse import quote_plus

# Load enviroment variables
load_dotenv()  # type: ignore

# VM credencials
host = os.getenv("VM_DB_HOST")
port = os.getenv("VM_DB_PORT") or "3306"
user = os.getenv("VM_DB_USER")
password = os.getenv("VM_DB_PASS")
db_name = os.getenv("VM_DB_NAME")

# Connection string
# URL-encode credentials to safely handle special characters in user/password
_user = quote_plus(user or "")
_pwd = quote_plus(password or "")
_db = db_name or ""
# include charset and ensure port has a default
engine = create_engine(f"mysql+pymysql://{_user}:{_pwd}@{host}:{port}/{_db}?charset=utf8mb4")

# Create database if not exists
with engine.connect()as conn:
    conn.execute(f"CREATE DATABASE IF NOT EXISTS {db_name}") # type: ignore
    conn.execute(f"USE {db_name}") # type: ignore

    # Radiology porcedures  data
    data = { # type: ignore
        "procedure_code": ["XR101", "CT202", "MRI303", "US404"],
        "procedure_name": ["X-Ray Spine", "CT Chest", "MRI Knee", "Ultrasound Liver"],
        # must match number of procedure rows (4)
        "duration_min": [12, 28, 50, 18]
    }

df = pd.DataFrame(data)
df.to_sql("radiology_procedures", con=engine, if_exists="replace", index=False)

# Read back
result = pd.read_sql("SELECT * FROM radiology_procedures", con=engine) # type: ignore
print(result)
print(f"Row count: {len(result)}")
      