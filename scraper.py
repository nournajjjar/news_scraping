# scraper.py
import os
import requests
from bs4 import BeautifulSoup
import mysql.connector
from mysql.connector import errorcode
from datetime import datetime
import time
import feedparser

# ---- Configuration (read from environment) ----
DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB_NAME")
}

FRANCE24_URL = "https://www.france24.com/fr/"

# ---- Fetch and parse France24 homepage ----




def fetch_france24():
    feeds = [
        "https://www.france24.com/fr/rss",
        "https://www.france24.com/fr/afrique/rss",
        "https://www.france24.com/fr/europe/rss",
        "https://www.france24.com/fr/économie/rss",
    ]

    articles = []
    for feed_url in feeds:
        feed = feedparser.parse(feed_url)
        for entry in feed.entries:
            articles.append({
                "title": entry.title,
                "link": entry.link,
                "category": feed.feed.title
            })
    return articles



# ---- Save to MySQL, skip duplicates using UNIQUE index ----
def save_to_mysql(items):
    if not DB_CONFIG["host"]:
        raise RuntimeError("DB_HOST is not set in environment variables.")
    conn = None
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()
        insert_sql = """
            INSERT INTO france24_articles (title, link, category)
            VALUES (%s, %s, %s)
        """
        for item in items:
            try:
                cursor.execute(insert_sql, (item["title"], item["link"], item["category"]))
            except mysql.connector.IntegrityError as e:
                # Duplicate link -> skip
                if e.errno == errorcode.ER_DUP_ENTRY:
                    # optional: update scraped_at if desired
                    continue
                else:
                    raise
        conn.commit()
        cursor.close()
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    try:
        articles = fetch_france24()
        print(f"[{datetime.now().isoformat()}] Fetched {len(articles)} candidate articles.")
        save_to_mysql(articles)
        print("✅ Saved to MySQL (duplicates are skipped).")
    except Exception as e:
        print("❌ Error:", str(e))
        raise
