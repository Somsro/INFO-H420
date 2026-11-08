import json
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from airflow.sdk import dag, task
from pathlib import Path
from pymupdf4llm import to_markdown
from openai import OpenAI
from config import PAPERS_DIR, MARKDOWN_DIR
from db import get_processed_filenames, save_paper, get_all_papers


PROMPT_TEMPLATE = """You are an assistant that analyzes research papers.

Read the paper below (converted to Markdown) and return ONLY a valid JSON object
with exactly this structure:

{{
  "title": "Paper title",
  "summary": "A short summary of the paper (3-5 sentences).",
  "keywords": ["keyword one", "keyword two"],
  "category": "Data Engineering"
}}

Rules:
- "title": the paper's title, as a string.
- "summary": a short summary of the paper, as a string.
- "keywords": a list of 2 keywords, as strings.
- "category": a single short category (e.g. "Data Engineering", "Machine Learning",
  "Responsible AI", "Process Modeling").
- Do not add any text, explanation, or Markdown code fences outside the JSON object.

Paper:
\"\"\"
{paper_markdown}
\"\"\"
"""

REQUIRED_FIELDS = {"title": str, "summary": str, "keywords": list, "category": str}

def _validate(data: dict) -> None:
    for field, expected_type in REQUIRED_FIELDS.items():
        if field not in data:
            raise ValueError(f"Missing field: {field}")
        if not isinstance(data[field], expected_type):
            raise ValueError(f"Field '{field}' should be of type {expected_type.__name__}")




@task
def pdf_to_markdown(pdf_path: str) -> dict:
    """Converts a PDF to Markdown, then saves it in MARKDOWN_DIR, returning a dict with the file name and the file path of the Markdown."""
    path = Path(pdf_path)
    markdown = to_markdown(str(path))

    MARKDOWN_DIR.mkdir(parents=True, exist_ok=True)
    md_path = MARKDOWN_DIR / path.with_suffix(".md").name
    md_path.write_text(markdown, encoding="utf-8")

    print(f"Converted {path.name} -> {md_path} ({len(markdown)} characters)")
    return {"filename": path.name, "md_path": str(md_path)}


@task(retries=1, retry_delay=timedelta(seconds=5)) #We need to specify the retry_delay because it's 5min by default
def llm_summary(paper: dict) -> dict:
    """Call the local LLM and return the parsed metadata as a dict.

    Raises on invalid JSON or missing fields; Airflow retries the task once.

    We consider the retry is not only for the json but also for the llm call itself.
    """
    paper_markdown = Path(paper["md_path"]).read_text(encoding="utf-8")
    client = OpenAI(
        base_url="http://localhost:11434/v1",
        api_key="ollama",
    )

    # Truncate to avoid exceeding the model's context window
    prompt = PROMPT_TEMPLATE.format(paper_markdown=paper_markdown[:12000])

    response = client.chat.completions.create(
        model="llama3.2",
        messages=[{"role": "user", "content": prompt}],
        response_format={"type": "json_object"},  # JSON mode
    )

    data = json.loads(response.choices[0].message.content)  # raises JSONDecodeError if invalid
    _validate(data)  # raises ValueError if fields are missing or wrongly typed
    return {"filename": paper["filename"], **data}

@task
def store_paper(paper: dict) -> dict: # remplace save_json mais considère la metadonnée
    """Save one paper's metadata to the database and pass it on for the notification."""
    metadata = {key: paper[key] for key in ("title", "summary", "keywords", "category")}
    save_paper(paper["filename"], metadata)
    return paper

@task
def find_new_pdfs() -> list[str]: # cette fonction utilise get_processed_filenames() définie dans db.py
    """Return the full paths of PDFs in the papers folder that are not yet in the database."""
    if not PAPERS_DIR.exists():
        raise FileNotFoundError(f"Papers folder not found: {PAPERS_DIR}")

    processed = get_processed_filenames()
    all_pdfs = sorted(PAPERS_DIR.glob("*.pdf"))
    new_pdfs = [str(pdf) for pdf in all_pdfs if pdf.name not in processed]

    print(f"Found {len(all_pdfs)} PDFs, {len(new_pdfs)} new: {[Path(p).name for p in new_pdfs]}")
    return new_pdfs

@task(trigger_rule="all_done")
def build_html_page() -> None:
    """Regenerate the static HTML page from every paper in the database.

    Runs even in case of failure.
    """
    papers = get_all_papers()
    print(f"[stub] would build the HTML page with {len(papers)} papers")


@task
def notify_new_papers(new_papers: list[dict]) -> None: # il faut ajouter les fonctionnalités Discord ici, mais pour l'instant on se contente d'un print
    """Send a Discord notification listing the newly processed papers."""
    titles = [paper["title"] for paper in new_papers]
    print(f"[stub] would notify Discord about {len(titles)} new papers: {titles}")


@dag(
    dag_id="paper_pipeline",
    description="Turn new research PDFs into a searchable study library",
    schedule=timedelta(minutes=5),
    start_date=datetime(2026, 11, 6, tzinfo=ZoneInfo("Europe/Brussels")),
    catchup=False,
    max_active_runs=1,  # never run two pipelines at once
)
def paper_pipeline():
    """Scan the papers folder, process each new PDF, then update the HTML and notify Discord."""
    """
    Faire un research library synced with google drive (accessible par le code)
    Check comment le dag est trigger (X minutes ou bien dès que y a un nouveau pdf dans le dossier)
    Check if pdf is already summarized (maybe have 2 folders, one done and to be done OR use html file)
    corriger le summary du llm (prompt buggé ?)
    build la db
    créer le html comme ils veulent
    discord
    """
    new_pdfs = find_new_pdfs()

    # Fan-out: one task instance per new PDF, chained by map index
    converted = pdf_to_markdown.expand(pdf_path=new_pdfs)
    metadata = llm_summary.expand(paper=converted)
    stored = store_paper.expand(paper=metadata)

    # Fan-in: these run once, after all papers are processed
    html = build_html_page()
    notification = notify_new_papers(stored)

    stored >> html >> notification


paper_pipeline()