import requests
import pandas as pd
import os
from dotenv import load_dotenv
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

load_dotenv()
TOKEN = os.getenv("BOT_TOKEN")
USER = os.getenv("USER")
PASSWORD = os.getenv("PASSWORD")
ACCOUNT = os.getenv("ACCOUNT")   # example: abc12345.ap-south-1.aws

WAREHOUSE = os.getenv("WAREHOUSE")
DATABASE = os.getenv("DATABASE")
SCHEMA = os.getenv("SCHEMA")
TABLE_NAME3 = os.getenv("TABLE_NAME3")

BASE_URL = f"https://api.telegram.org/bot{TOKEN}"

response = requests.get(f"{BASE_URL}/getUpdates")
data = response.json()

chat_ids = []

for update in data.get("result", []):
    if "message" in update:
        msg = update["message"]

        # Optional: filter only /start users
        if msg.get("text") == "/start":
            chat_id = msg["chat"]["id"]
            chat_ids.append(chat_id)

# =========================
# CREATE DATAFRAME
# =========================
df = pd.DataFrame(chat_ids, columns=["CHAT_ID"])

# Remove duplicates
df = df.drop_duplicates()
df = df.reset_index(drop=True)

conn = snowflake.connector.connect(
    user=USER,
    password=PASSWORD,
    account=ACCOUNT,
    warehouse=WAREHOUSE,
    database=DATABASE,
    schema=SCHEMA
)

# =========================
# UPLOAD DATAFRAME
# =========================
success, nchunks, nrows, _ = write_pandas(
    conn,
    df,
    table_name=TABLE_NAME3
)
cursor = conn.cursor()

cursor.execute(f"""
CREATE OR REPLACE TABLE {TABLE_NAME3} AS
SELECT 
    CHAT_ID,
    MAX(created_at) AS created_at
FROM {TABLE_NAME3}
GROUP BY CHAT_ID;
""")

cursor.close()
conn.close()