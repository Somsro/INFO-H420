from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from airflow.sdk import dag, task
from pathlib import Path
from pymupdf4llm import to_markdown


PROJECT_ROOT = Path(__file__).resolve().parents[1]


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


@dag(
    dag_id="paper_pipeline",
    description="PDF into Markdown",
    schedule=timedelta(seconds=30), #paper_pipeline() will be called every 30 seconds
    start_date=datetime(2026, 11, 6, tzinfo=ZoneInfo("Europe/Brussels")),
    catchup=False
)
def paper_pipeline():
    pdf = "assignment3.pdf"

    converted = pdf_to_markdown(pdf_path=pdf)
    md_path = save_markdown(converted)
    print(f"Markdown file saved at: {md_path}")

    


paper_pipeline()