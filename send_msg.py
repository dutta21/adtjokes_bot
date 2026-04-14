import snowflake.connector
import requests
import os
from dotenv import load_dotenv

load_dotenv()
TOKEN = os.getenv("BOT_TOKEN")
USER = os.getenv("USER")
PASSWORD = os.getenv("PASSWORD")
ACCOUNT = os.getenv("ACCOUNT")   # example: abc12345.ap-south-1.aws

WAREHOUSE = os.getenv("WAREHOUSE")
DATABASE = os.getenv("DATABASE")
SCHEMA = os.getenv("SCHEMA")
TABLE_NAME3 = os.getenv("TABLE_NAME3")
BOT_TOKEN = os.getenv("BOT_TOKEN")
BASE_URL = f"https://api.telegram.org/bot{BOT_TOKEN}"


# =========================
# FETCH USERS
# =========================
def fetch_chat_ids():
    conn = snowflake.connector.connect(
    user=USER,
    password=PASSWORD,
    account=ACCOUNT,
    warehouse=WAREHOUSE,
    database=DATABASE,
    schema=SCHEMA
)

    cursor = conn.cursor()

    cursor.execute(f"SELECT CHAT_ID FROM {TABLE_NAME3}")
    chat_ids = [row[0] for row in cursor.fetchall()]

    cursor.close()
    conn.close()

    return chat_ids

# =========================
# REMOVE USER
# =========================
def remove_user(chat_id):
    conn = snowflake.connector.connect(user=USER,
    password=PASSWORD,
    account=ACCOUNT,
    warehouse=WAREHOUSE,
    database=DATABASE,
    schema=SCHEMA)
    cursor = conn.cursor()

    cursor.execute(
        f"DELETE FROM {TABLE_NAME3} WHERE CHAT_ID = %s",
        (chat_id,)
    )

    conn.commit()
    cursor.close()
    conn.close()

# =========================
# SEND MESSAGE
# =========================
def send_message(chat_id, message):
    try:
        response = requests.post(
            f"{BASE_URL}/sendMessage",
            data={
                "chat_id": chat_id,
                "text": message
            }
        )

        result = response.json()

        if not result.get("ok"):
            remove_user(chat_id)

    except:
        remove_user(chat_id)

# =========================
# MAIN
# =========================
def main(messagex):
    message = messagex

    chat_ids = fetch_chat_ids()

    for chat_id in chat_ids:
        send_message(chat_id, message)


if __name__ == "__main__":
    main()