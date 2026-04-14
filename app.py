import pandas as pd
import requests
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import os
from dotenv import load_dotenv
from datetime import datetime, timezone
import send_msg

url = "https://v2.jokeapi.dev/joke/Dark"

load_dotenv()

USER = os.getenv("USER")
PASSWORD = os.getenv("PASSWORD")
ACCOUNT = os.getenv("ACCOUNT")   # example: abc12345.ap-south-1.aws

WAREHOUSE = os.getenv("WAREHOUSE")
DATABASE = os.getenv("DATABASE")
SCHEMA = os.getenv("SCHEMA")
TABLE_NAME1 = os.getenv("TABLE_NAME1")
TABLE_NAME2 = os.getenv("TABLE_NAME2")

response = requests.get(url)
data = response.json()
df = pd.json_normalize(data)
df["LOAD_TIME"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
df.columns = [col.replace(".", "_") for col in df.columns]

if "joke" in df.columns:
    x="Joke: "+df['joke']
    send_msg.main(x)

    conn = snowflake.connector.connect(
        user=USER,
        password=PASSWORD,
        account=ACCOUNT
    )

    cur = conn.cursor()

    try:
        
        cur.execute(f'USE WAREHOUSE "{WAREHOUSE}"')
        cur.execute(f'USE DATABASE "{DATABASE}"')
        cur.execute(f'USE SCHEMA "{SCHEMA}"')

        
        col_defs = []
        for col, dtype in df.dtypes.items():
            if "int" in str(dtype):
                col_defs.append(f'"{col}" INT')
            elif "float" in str(dtype):
                col_defs.append(f'"{col}" FLOAT')
            else:
                col_defs.append(f'"{col}" STRING')

        create_table_sql = f'''
        CREATE TABLE IF NOT EXISTS "{TABLE_NAME1}" (
            {", ".join(col_defs)}
        )
        '''
        cur.execute(create_table_sql)

        success, nchunks, nrows, _ = write_pandas(conn, df, TABLE_NAME1)

        # print("Upload Success:", success)
        # print("Chunks Uploaded:", nchunks)
        # print("Rows Inserted:", nrows)

        
        cur.execute(f'SELECT COUNT(*) FROM "{TABLE_NAME1}"')

    finally:
        cur.close()
        conn.close()
else:
    x="Joke: \nSetup:"+df['setup']+"\n\nDelivey:"+df['delivery']
    send_msg.main(x)
    conn = snowflake.connector.connect(
        user=USER,
        password=PASSWORD,
        account=ACCOUNT
    )

    cur = conn.cursor()

    try:
        
        cur.execute(f'USE WAREHOUSE "{WAREHOUSE}"')
        cur.execute(f'USE DATABASE "{DATABASE}"')
        cur.execute(f'USE SCHEMA "{SCHEMA}"')

        
        col_defs = []
        for col, dtype in df.dtypes.items():
            if "int" in str(dtype):
                col_defs.append(f'"{col}" INT')
            elif "float" in str(dtype):
                col_defs.append(f'"{col}" FLOAT')
            elif "datetime" in str(dtype):
                col_defs.append(f'"{col}" TIMESTAMP_NTZ')
            else:
                col_defs.append(f'"{col}" STRING')

        create_table_sql = f'''
        CREATE TABLE IF NOT EXISTS "{TABLE_NAME2}" (
            {", ".join(col_defs)}
        )
        '''
        cur.execute(create_table_sql)

        success, nchunks, nrows, _ = write_pandas(conn, df, TABLE_NAME2)

        # print("Upload Success:", success)
        # print("Chunks Uploaded:", nchunks)
        # print("Rows Inserted:", nrows)

        cur.execute(f'SELECT COUNT(*) FROM "{TABLE_NAME2}"')

    finally:
        cur.close()
        conn.close()

