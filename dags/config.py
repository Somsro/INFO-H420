"""Central config. It is read from the .env file at the repo root."""

import os
from pathlib import Path

from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(REPO_ROOT / ".env")

DRIVE_ROOT = Path(os.environ["DRIVE_ROOT"])
PAPERS_DIR = DRIVE_ROOT / "papers"
OUTPUT_DIR = DRIVE_ROOT / "output"

DATA_DIR = REPO_ROOT / "data"
MARKDOWN_DIR = DATA_DIR / "markdown"
DB_PATH = DATA_DIR / "papers.db"

HTML_PATH = OUTPUT_DIR / "index.html"

DISCORD_WEBHOOK_URL = os.environ.get("DISCORD_WEBHOOK_URL")