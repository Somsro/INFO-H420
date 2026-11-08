"""Central config. It is read from the .env file at the repo root."""

#  vous pourrez ajouter d'autres variables d'environnement ici, par exemple pour la base de données, le chemin vers les fichiers, etc.

import os
from pathlib import Path

from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(REPO_ROOT / ".env")

DRIVE_ROOT = Path(os.environ["DRIVE_ROOT"])
PAPERS_DIR = DRIVE_ROOT / "papers"
OUTPUT_DIR = DRIVE_ROOT / "output"


MARKDOWN_DIR = REPO_ROOT / "data" / "markdown"

