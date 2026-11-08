"""Central config. It is read from the .env file at the repo root."""

#  vous pourrez ajouter d'autres variables d'environnement ici, par exemple pour la base de données, le chemin vers les fichiers, etc.

import os
from pathlib import Path

from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(REPO_ROOT / ".env")

PAPERS_DIR = Path(os.environ["PAPERS_DIR"])

MARKDOWN_DIR = REPO_ROOT / "data" / "markdown"