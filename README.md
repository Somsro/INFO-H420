# Research Library Pipeline (INFO-H420, Assignment 3)

An Apache Airflow pipeline that turns research PDFs into a searchable study library.
It scans a Google Drive-synced folder for new PDFs, converts them to Markdown, asks a
locally served LLM for a title, summary, keywords and category, stores the result in
SQLite, regenerates a static HTML page, and sends a Discord notification.

## How it works

The DAG `paper_pipeline` runs every 5 minutes (one run at a time):

```
find_new_pdfs ──► pdf_to_markdown ──► llm_summary ──► store_paper ──► build_html_page ──► notify_new_papers
                  └──────── one task instance per new PDF ────────┘
```

| Task | What it does |
|---|---|
| `find_new_pdfs` | Lists the PDFs in `papers/` that are not yet in the database |
| `pdf_to_markdown` | Converts each PDF to Markdown with PyMuPDF4LLM (saved locally in `data/markdown/`) |
| `llm_summary` | Sends the start of the paper to the local LLM (Ollama, JSON mode), parses and validates the JSON; retried once on failure |
| `store_paper` | Saves the metadata to the local SQLite database |
| `build_html_page` | Regenerates `output/index.html` from the database (runs even if one paper failed) |
| `notify_new_papers` | Sends a Discord message listing the new papers |

The processing tasks use Airflow dynamic task mapping (`.expand()`): one instance per
new PDF. If there are no new PDFs, they are skipped and the run ends cleanly.

## Repository structure

```
.
├── dags/
│   ├── paper_pipeline.py   # the Airflow DAG and its tasks
│   ├── config.py           # paths and settings, read from .env
│   ├── db.py               # SQLite storage
│   └── html_page.py        # static HTML generation
├── data/                   # local only, not committed (Markdown files, database)
├── .env.example            # template for your .env
└── requirements.txt
```

Synced Google Drive folder (location set by `DRIVE_ROOT`):

```
ResearchLibrary/
├── papers/      # put PDFs here
└── output/
    └── index.html
```

The SQLite database stays on the local machine, outside the synced folder.

## Requirements

- Linux, macOS, or Windows with WSL2 (Airflow does not run natively on Windows)
- Python 3.10 or newer
- [Ollama](https://ollama.com)
- A Google Drive folder synced to your machine (or any local folder for testing)

## Setup

**1. Clone the repo and create a virtual environment**

```bash
git clone https://github.com/Somsro/INFO-H420.git
cd INFO-H420
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
```

**2. Install Airflow** with the official constraints file (replace the version with the one below if different):

```bash
AIRFLOW_VERSION=3.4.0
PYTHON_VERSION="$(python -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')"
pip install "apache-airflow==${AIRFLOW_VERSION}" \
  --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
```

**3. Install the other dependencies**

```bash
pip install -r requirements.txt
```

**4. Install Ollama and download the model**

```bash
curl -fsSL https://ollama.com/install.sh | sh    # on Ubuntu, install zstd first: sudo apt install zstd
ollama pull llama3.2
```

**5. Create your `.env`** at the repo root, based on `.env.example`:

```
DRIVE_ROOT="/path/to/ResearchLibrary"
# PLAZMA: Discord webhook variable
```

`DRIVE_ROOT` is the synced folder containing `papers/` and `output/`.
Never commit `.env`: it contains machine-specific paths and the Discord webhook URL.

**6. Point Airflow at this repo**

```bash
export AIRFLOW_HOME="$PWD/.airflow"
export AIRFLOW__CORE__DAGS_FOLDER="$PWD/dags"
export AIRFLOW__CORE__LOAD_EXAMPLES=False
```

Add these lines to `.venv/bin/activate` to set them automatically on every activation.

## Running

Make sure Ollama is running (`curl http://localhost:11434` should print "Ollama is running"), then:

```bash
airflow standalone
```

Open http://localhost:8080 and log in with the username and password printed in the
terminal. Unpause the `paper_pipeline` DAG: it will run every 5 minutes. Drop a PDF into
`papers/` and it will be processed on the next run. You can also trigger a run manually
with the play button.

To run the whole pipeline once from the command line, without the scheduler:

```bash
airflow dags test paper_pipeline
```

Results:

- Library page: `DRIVE_ROOT/output/index.html` (search box and category filter)
- Database: `REPO_ROOT/data/papers.db`
- A Discord message for each run that processed new papers

## Notes and limitations

- Only the first 12,000 characters of each paper are sent to the LLM, to stay within the
  model's context window. Title, abstract and introduction are enough for the metadata.
- With a 3B model on a laptop CPU, each paper takes a few minutes to summarize.
- The PDF links on the HTML page work when the page is opened from the synced folder,
  not from the Google Drive web interface.
- A PDF that fails to process is not saved, so it is retried on the next run.


## Google Drive syncing
We used Google Drive for desktop to sync the papers folder. The folder is called "Research Library" and it contains two subfolders: "papers" and "output". The "papers" folder is where we put the PDFs to be processed, and the "output" folder is where the generated HTML page is saved. The syncing ensures that the files are available on all devices connected to the same Google account.

## Ollama use
After converting the pdf into a markdown file (using pymupdf4llm library), we send it to the local llm (Ollama) to generate a title, summary, keywords and category. The prompt is the following :
You are an assistant that analyzes research papers.

Read the paper below (converted to Markdown) and return ONLY a valid JSON object
with this structure:
The values shown below are placeholders only.
You MUST replace every placeholder with information extracted from the paper.
Never return "Paper title", "A short summary...", "keyword one", or "keyword two".

{{
  "title": "Paper title",
  "summary": "A short summary of the paper (3-5 sentences).",
  "keywords": ["keyword one", "keyword two"],
  "category": "Choose a catergory based on the paper"
}}

Strictly follow the rules:
\- "title": the paper's title, as a string.  
\- "summary": a short summary of the paper, as a string.  
\- "keywords": an array containing a few strings.  
  Select the two most important keywords from the paper. Do not include any  
  additional keywords, sub-keywords, phrases, or explanations in this array.  
\- "category": a single short category (e.g. "Data Engineering", "Machine Learning",  
  "Responsible AI", "Process Modeling", etc...) Don't exclude any other possible
  categories.  
\- Before returning the JSON, count the items in "keywords" and verify that the
  array contains exactly 2 items.  
\- Do not add any text, explanation, or Markdown code fences outside the JSON object.  

Paper:
\"\"\"
<Actual paper content in .md format>
\"\"\"
"""

Note : the category, keywords and summary are in the same language as the paper.