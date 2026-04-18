# 🤖 adtjokes_bot

A Telegram bot that automatically delivers random jokes to subscribers at regular intervals — powered by the [JokeAPI](https://v2.jokeapi.dev/), stored in Snowflake, and fully automated via GitHub Actions.

---

## 📌 How It Works

1. **Users subscribe** by sending `/start` to the Telegram bot.
2. **`fetch_ids.py`** periodically fetches new subscriber chat IDs from Telegram and stores them in Snowflake.
3. **`app.py`** fetches a random joke from JokeAPI, sends it to all subscribers via Telegram, and logs the joke data to Snowflake.
4. **GitHub Actions** orchestrates and schedules all of the above automatically.

---

## 🗂️ Project Structure

```
adtjokes_bot/
└── main/
    ├── app.py               # Fetches joke, sends to subscribers, logs to Snowflake
    ├── fetch_ids.py         # Fetches new /start chat IDs from Telegram → Snowflake
    ├── send_msg.py          # Helper: reads chat IDs from Snowflake and sends messages
    └── .github/
        └── workflows/
            ├── ingest.yml   # Scheduled workflow to fetch & send jokes
            └── update.yml   # Scheduled workflow to update subscriber IDs
```

---

## ⚙️ Setup

### Prerequisites

- Python 3.8+
- A [Telegram Bot Token](https://core.telegram.org/bots#botfather) (create one via BotFather)
- A [Snowflake](https://www.snowflake.com/) account with a warehouse, database, and schema set up

### Install Dependencies

```bash
pip install pandas requests snowflake-connector-python python-dotenv
```

### Environment Variables

All secrets and config values are read from environment variables. Create a `.env` file locally (or set them as GitHub Actions secrets for automation):

| Variable       | Description                                      |
|----------------|--------------------------------------------------|
| `BOT_TOKEN`    | Telegram Bot API token                           |
| `USER`         | Snowflake username                               |
| `PASSWORD`     | Snowflake password                               |
| `ACCOUNT`      | Snowflake account identifier (e.g. `abc123.ap-south-1.aws`) |
| `WAREHOUSE`    | Snowflake warehouse name                         |
| `DATABASE`     | Snowflake database name                          |
| `SCHEMA`       | Snowflake schema name                            |
| `TABLE_NAME1`  | Snowflake table for single-line jokes            |
| `TABLE_NAME2`  | Snowflake table for two-part jokes (setup/delivery) |
| `TABLE_NAME3`  | Snowflake table for subscriber chat IDs          |

> **Note:** Uncomment the `load_dotenv()` lines in each file if running locally with a `.env` file.

---

## 🚀 Usage

### Fetch & Store New Subscriber IDs

Run this to collect chat IDs of users who sent `/start` to the bot and store them in Snowflake:

```bash
python fetch_ids.py
```

### Send a Joke to All Subscribers

Run this to fetch a random joke, broadcast it to all subscribers, and log it to Snowflake:

```bash
python app.py
```

---

## 🔄 Automation with GitHub Actions

Both scripts are scheduled and executed automatically using GitHub Actions workflows. The workflows run at configured intervals (e.g., daily or hourly) and use **GitHub Secrets** to securely inject the environment variables at runtime.

To configure:
1. Go to your repository → **Settings** → **Secrets and variables** → **Actions**
2. Add all the environment variables listed above as repository secrets.
3. The workflows in `.github/workflows/` will pick them up automatically.

---

## 🗃️ Snowflake Schema

The bot maintains three tables in Snowflake:

| Table          | Contents                                              |
|----------------|-------------------------------------------------------|
| `TABLE_NAME1`  | Single-line joke data from JokeAPI                   |
| `TABLE_NAME2`  | Two-part joke data (setup + delivery) from JokeAPI   |
| `TABLE_NAME3`  | Unique subscriber chat IDs (deduplicated)            |

Tables are created automatically on the first run if they don't already exist.

---

## 🛠️ Tech Stack

- **Python** — core scripting language
- **Telegram Bot API** — subscriber management and message delivery
- **JokeAPI** (`v2.jokeapi.dev`) — random joke source
- **Snowflake** — data storage for jokes and subscriber IDs
- **GitHub Actions** — scheduling and automation

---

## 📄 License

This project is open source. Feel free to fork and build upon it!
