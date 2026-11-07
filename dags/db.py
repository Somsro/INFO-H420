"""
TEMPORARY. It returns static data so the DAG can be built and tested.
NEXT. Replace the bodies with the real SQLite implementation, with the same
function names, parameters and return formats.
"""


def get_processed_filenames() -> set[str]:
    """Return the filenames (e.g. "paper.pdf" and not full paths) already saved in the database."""
    return {"example_paper.pdf"}


def save_paper(filename: str, metadata: dict) -> None:
    """Save one paper. metadata has keys: title, summary, keywords (list), category."""
    print(f"[stub] would save {filename}: {metadata['title']}")


def get_all_papers() -> list[dict]:
    """Return all papers, each a dict with keys:
    filename, title, summary, keywords (list of str), category.
    """
    return [
        {
            "filename": "example_paper_1.pdf",
            "title": "An Example Paper on Tropical Fish lmao",
            "summary": "This paper describes interesting fauna.",
            "keywords": ["tropical fish", "corals", "aquatic life"],
            "category": "Biology",
        },
        {
            "filename": "example_paper_2.pdf",
            "title": "Metacognition and piano learning",
            "summary": "The mechanisms behind piano learning",
            "keywords": ["piano", "learning", "music"],
            "category": "Didactics",
        },
    ]