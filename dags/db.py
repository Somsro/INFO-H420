import json
import sqlite3
from pathlib import Path
from config import DB_PATH

class PaperDatabase:
  """Manages SQLite storage for research paper metadata."""

  def __init__(self, db_path: str | Path = DB_PATH):
    self.db_path = Path(db_path)
    self.db_path.parent.mkdir(parents=True, exist_ok=True)
    self._init_db()

  def _get_connection(self) -> sqlite3.Connection:
    return sqlite3.connect(self.db_path)

  def _init_db(self) -> None:
    """Creates the database table schema if it does not already exist."""
    with self._get_connection() as conn:
      cursor = conn.cursor()
      cursor.execute("""
                CREATE TABLE IF NOT EXISTS papers (
                    filename TEXT PRIMARY KEY,
                    title TEXT NOT NULL,
                    summary TEXT NOT NULL,
                    keywords TEXT NOT NULL,
                    category TEXT NOT NULL
                )
            """)
      conn.commit()

  def get_processed_filenames(self) -> set[str]:
    """Return the set of filenames (e.g., 'paper.pdf') already saved in the database."""
    with self._get_connection() as conn:
      cursor = conn.cursor()
      cursor.execute("SELECT filename FROM papers")
      rows = cursor.fetchall()
      return {row[0] for row in rows}

  def save_paper(self, filename: str, metadata: dict) -> None:
    """Save or update a paper record.

    metadata keys: title, summary, keywords (list), category.
    """
    keywords_str = json.dumps(metadata.get("keywords", []))

    with self._get_connection() as conn:
      cursor = conn.cursor()
      cursor.execute(
          """
                INSERT INTO papers (filename, title, summary, keywords, category)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(filename) DO UPDATE SET
                    title = excluded.title,
                    summary = excluded.summary,
                    keywords = excluded.keywords,
                    category = excluded.category
            """,
          (
              filename,
              metadata.get("title", ""),
              metadata.get("summary", ""),
              keywords_str,
              metadata.get("category", ""),
          ),
      )
      conn.commit()

  def get_all_papers(self) -> list[dict]:
    """Return all stored papers with deserialized keyword lists."""
    with self._get_connection() as conn:
      cursor = conn.cursor()
      cursor.execute(
          "SELECT filename, title, summary, keywords, category FROM papers"
      )
      rows = cursor.fetchall()

      papers = []
      for row in rows:
        filename, title, summary, keywords_str, category = row
        try:
          keywords = json.loads(keywords_str)
        except (json.JSONDecodeError, TypeError):
          keywords = []

        papers.append({
            "filename": filename,
            "title": title,
            "summary": summary,
            "keywords": keywords,
            "category": category,
        })
      return papers