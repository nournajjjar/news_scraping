# scraper.py
import os
from datetime import datetime
import feedparser
import mysql.connector
from mysql.connector import errorcode
from dotenv import load_dotenv

# ---- Load environment variables from .env ----
load_dotenv()

DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB_NAME"),
    "port": int(os.getenv("DB_PORT", 3306))
}
try:
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()
    print("✅ Connected to cloud DB!")
except mysql.connector.Error as err:
    print("❌ Connection failed:", err)
    

# ---- France24 RSS feeds ----
FRANCE24_FEEDS = [
    "https://www.france24.com/fr/rss",
    "https://www.france24.com/fr/afrique/rss",
    "https://www.france24.com/fr/europe/rss",
    "https://www.france24.com/fr/économie/rss",
]

# ---- Fetch articles from France24 ----
def fetch_france24():
    articles = []
    for feed_url in FRANCE24_FEEDS:
        feed = feedparser.parse(feed_url)
        for entry in feed.entries:
            articles.append({
                "title": entry.title,
                "link": entry.link,
                "summary": entry.get("summary", ""),  # summary/description
                "category": feed.feed.get("title", "")
            })
    return articles

# ---- Save articles to MySQL, skip duplicates using UNIQUE index ----
def save_to_mysql(articles):
    import mysql.connector

    # Optional: sanity check
    if not DB_CONFIG["host"]:
        print("DB host not set!")
        return

    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()

    # Create table if it doesn't exist
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS france24_articles (
        id INT AUTO_INCREMENT PRIMARY KEY,
        title VARCHAR(500),
        link VARCHAR(1000),
        summary TEXT,
        category VARCHAR(255),
        published_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )
    """
    cursor.execute(create_table_sql)

    insert_sql = """
    INSERT INTO france24_articles (title, link, summary, category)
    VALUES (%s, %s, %s, %s)
    """
    for item in articles:
        cursor.execute(insert_sql, (item["title"], item["link"], item["summary"], item["category"]))

    conn.commit()
    cursor.close()
    conn.close()
    print("✅ Articles saved to MySQL!")


# ---- Main execution ----
if __name__ == "__main__":
    try:
        articles = fetch_france24()
        print(f"[{datetime.now().isoformat()}] Fetched {len(articles)} candidate articles.")
        save_to_mysql(articles)
        print("✅ Saved to MySQL (duplicates are skipped).")
    except Exception as e:
        print("❌ Error:", str(e))
        raise
