import json
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from airflow.sdk import dag, task
from pathlib import Path
from pymupdf4llm import to_markdown
from openai import OpenAI
from config import PAPERS_DIR, MARKDOWN_DIR, HTML_PATH
from discord_webhook import send_discord_webhook
from db import PaperDatabase
from html_page import build_html

PROMPT_TEMPLATE = """You are an assistant that analyzes research papers.

Read the paper below (converted to Markdown) and return ONLY a valid JSON object
with this structure:
The values shown below are placeholders only.
You MUST replace every placeholder with information extracted from the paper.
Never return "Paper title", "A short summary...", "keyword one", or "keyword two".

{{
  "title": "Paper title",
  "summary": "A short summary of the paper (3-5 sentences).",
  "keywords": ["keyword one", "keyword two"],
  "category": "Choose a category based on the paper"
}}

Strictly follow the rules:
- "title": the paper's title, as a string.
- "summary": a short summary of the paper, as a string.
- "keywords": an array containing a few strings.
  Select the two most important keywords from the paper. Do not include any
  additional keywords, sub-keywords, phrases, or explanations in this array.
- "category": a single short category (e.g. "Data Engineering", "Machine Learning",
  "Responsible AI", "Process Modeling", etc...) Don't exclude any other possible
  categories.
- Before returning the JSON, count the items in "keywords" and verify that the
  array contains exactly 2 items.
- Do not add any text, explanation, or Markdown code fences outside the JSON object.

Paper:
\"\"\"
{paper_markdown}
\"\"\"
"""

REQUIRED_FIELDS = {"title": str, "summary": str, "keywords": list, "category": str}

def _validate(data: dict) -> None:
    """ Raise error in case the data returned by the llm is not valid (missing fields or wrong types)."""
    for field, expected_type in REQUIRED_FIELDS.items():
        if field not in data:
            raise ValueError(f"Missing field: {field}")
        if not isinstance(data[field], expected_type):
            raise ValueError(f"Field '{field}' should be of type {expected_type.__name__}")

@task
def init_database() -> None:
  """Initialize SQLite schema at pipeline execution start."""
  PaperDatabase()

@task
def pdf_to_markdown(pdf_path: str) -> dict:
    """Converts a PDF to Markdown using pymupdf4llm library.
    Creates a temporary file in MARKDOWN_DIR (deleted after it the llm made the summary).
    Returns a dict with the file name and the file path of the Markdown."""
    path = Path(pdf_path)
    markdown = to_markdown(str(path))

    MARKDOWN_DIR.mkdir(parents=True, exist_ok=True)
    md_path = MARKDOWN_DIR / path.with_suffix(".md").name
    md_path.write_text(markdown, encoding="utf-8")

    print(f"Converted {path.name} -> {md_path} ({len(markdown)} characters)")
    return {"filename": path.name, "md_path": str(md_path)}


@task(retries=1, retry_delay=timedelta(seconds=5)) #We need to specify the retry_delay because it is 5min by default
def llm_summary(paper: dict) -> dict:
    """Call the local LLM and return the parsed metadata as a dict.

    Raises on invalid JSON or missing fields; Airflow retries the task once.
    We consider the retry is not only for the json but also for the llm call itself.

    The associated markdown file is deleted at the end of this task.

    Returns a dict with the original filename and the metadata fields: title, summary, keywords, category.
    """
    md_path = Path(paper["md_path"])
    paper_markdown = md_path.read_text(encoding="utf-8")
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

    md_path.unlink()
    print(f"Deleted temporary Markdown file: {md_path}")


    return {"filename": paper["filename"], **data}

@task
def store_paper(paper: dict) -> dict:
    """Save one paper's metadata to the database and pass it on for the notification."""
    metadata = {
        key: paper[key] for key in ("title", "summary", "keywords", "category")
    }
    db = PaperDatabase()
    db.save_paper(paper["filename"], metadata)
    return paper

@task
def find_new_pdfs() -> list[str]:
    """Return the full paths of PDFs in the papers folder that are not yet in the database."""
    if not PAPERS_DIR.exists():
        raise FileNotFoundError(f"Papers folder not found: {PAPERS_DIR}")

    db = PaperDatabase()
    processed = db.get_processed_filenames()
    all_pdfs = sorted(PAPERS_DIR.glob("*.pdf"))
    new_pdfs = [str(pdf) for pdf in all_pdfs if pdf.name not in processed]

    print(
        f"Found {len(all_pdfs)} PDFs, {len(new_pdfs)} new:"
        f" {[Path(p).name for p in new_pdfs]}"
    )
    return new_pdfs

@task(trigger_rule="all_done")
def build_html_page() -> None:
    """Regenerate the static HTML page from every paper in the database.

    Runs even if some papers failed, so one bad PDF can't block the page.
    """
    db = PaperDatabase()
    papers = db.get_all_papers()
    build_html(papers, HTML_PATH)
    print(f"Wrote HTML page with {len(papers)} papers to {HTML_PATH}")



@task
def notify_new_papers(new_papers: list[dict]) -> None:
  """Airflow task that sends Discord notifications."""
  if not new_papers:
    print("No new papers processed. Skipping Discord notification.")
    return

  send_discord_webhook(new_papers)
  print(f"Sent Discord notification for {len(new_papers)} papers.")


@dag(
    dag_id="paper_pipeline",
    description="Turn new research PDFs into a searchable study library",
    schedule=timedelta(minutes=5), # run every 5 minutes
    start_date=datetime(2026, 11, 14, tzinfo=ZoneInfo("Europe/Brussels")),
    catchup=False,
    max_active_runs=1,  # never run two pipelines at once
)
def paper_pipeline():
    """Scan the papers folder, process each new PDF, then update the HTML and notify Discord."""
    # Setup DB schema step
    init_db_step = init_database()
    new_pdfs = find_new_pdfs()

    # Enforce schema check prior to searching/processing PDFs
    init_db_step >> new_pdfs

    # Fan-out: one task instance per new PDF, chained by map index
    converted = pdf_to_markdown.expand(pdf_path=new_pdfs)
    metadata = llm_summary.expand(paper=converted)
    stored = store_paper.expand(paper=metadata)

    # Fan-in: these run once, after all papers are processed
    html = build_html_page()
    notification = notify_new_papers(stored)

    stored >> html >> notification


paper_pipeline()