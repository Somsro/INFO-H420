import json
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from airflow.sdk import dag, task
from pathlib import Path
from pymupdf4llm import to_markdown
from openai import OpenAI


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
  "category": "Data Engineering"
}}

Strictly follow the rules:
- "title": the paper's title, as a string.
- "summary": a short summary of the paper, as a string.
- "keywords": an array containing EXACTLY TWO strings: no more and no fewer.
  Select the two most important keywords from the paper. Do not include any
  additional keywords, sub-keywords, phrases, or explanations in this array.
- "category": a single short category (e.g. "Data Engineering", "Machine Learning",
  "Responsible AI", "Process Modeling").
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
    for field, expected_type in REQUIRED_FIELDS.items():
        if field not in data:
            raise ValueError(f"Missing field: {field}")
        if not isinstance(data[field], expected_type):
            raise ValueError(f"Field '{field}' should be of type {expected_type.__name__}")
    if len(data["keywords"]) != 2:
        raise ValueError("Field 'keywords' should contain exactly 2 keywords")


@task
def save_markdown(converted: dict[str, str]) -> str:
    """Save le Markdown à côté du PDF source."""
    pdf_path = Path(converted["filename"])
    output_path = pdf_path.with_suffix(".md") #Transforms the .pdf into .md

    output_path.write_text(converted["markdown"], encoding="utf-8")
    return str(output_path)


@task
def pdf_to_markdown(pdf_path: str) -> dict[str, str]: #We need str since airflow only accepts str as input/output of tasks
    """Convertit un PDF en Markdown."""
    path = Path(pdf_path)
    markdown = to_markdown(path)

    return {
        "filename": path.name, #Returns the whole path of the file as a string (with the .pdf extension)
        "markdown": markdown, #Content of the md file
    }



@task(retries=1, retry_delay=timedelta(seconds=5)) #We need to specify the retry because it's 5min by default
def llm_summary(paper_markdown: str) -> dict:
    """Call the local LLM and return the parsed metadata as a dict.

    Raises on invalid JSON or missing fields; Airflow retries the task once.

    We consider the retry is not only for the json but also for the llm call itself.
    """
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
    return data

@task
def save_json(data: dict) -> str:
    """Save the JSON data to a file (in data folder)."""
    data_dir = Path(__file__).resolve().parent / "data"
    data_dir.mkdir(parents=True, exist_ok=True)

    output_path = data_dir / "paper_summary.json"
    output_path.write_text(
        json.dumps(data, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    return str(output_path)


@dag(
    dag_id="paper_pipeline",
    description="PDF into Markdown",
    schedule=timedelta(seconds=30), #paper_pipeline() will be called every 30 seconds
    start_date=datetime(2026, 11, 6, tzinfo=ZoneInfo("Europe/Brussels")),
    catchup=False
)
def paper_pipeline():
    pdf = "assignment3.pdf"

    """
    Faire un research library synced with google drive (accessible par le code)
    Check comment le dag est trigger (X minutes ou bien dès que y a un nouveau pdf dans le dossier)
    Check if pdf is already summarized (maybe have 2 folders, one done and to be done OR use html file)
    corriger le summary du llm (prompt buggé ?)
    build la db
    créer le html comme ils veulent
    discord
    """

    #Convert pdf to md
    converted = pdf_to_markdown(pdf_path=pdf)
    md_path = save_markdown(converted)
    print(f"Markdown file saved at: {md_path}")

    #Call the LLM to summarize the paper and save the JSON output
    data = llm_summary(converted["markdown"])
    json_path = save_json(data)
    print(f"JSON saved at: {json_path}")



paper_pipeline()