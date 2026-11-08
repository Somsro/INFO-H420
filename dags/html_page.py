"""Generate the static HTML page listing every paper in the library."""
import html
import os
from datetime import datetime
from pathlib import Path
from string import Template
from urllib.parse import quote

PAGE_TEMPLATE = Template("""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Research Library</title>
<style>
  :root {
    --bg: #f6f7f9; --card: #ffffff; --text: #1d2330; --muted: #5b6475;
    --accent: #2f5bd3; --chip: #eef1f7; --border: #dde1e8;
  }
  @media (prefers-color-scheme: dark) {
    :root {
      --bg: #14171c; --card: #1d2128; --text: #e6e9ef; --muted: #9aa3b2;
      --accent: #7ea2ff; --chip: #2a303a; --border: #2f3540;
    }
  }
  * { box-sizing: border-box; }
  body {
    margin: 0; background: var(--bg); color: var(--text); line-height: 1.5;
    font-family: system-ui, -apple-system, "Segoe UI", Roboto, sans-serif;
  }
  main { max-width: 900px; margin: 0 auto; padding: 2rem 1rem; }
  h1 { margin: 0 0 0.25rem; }
  .meta { color: var(--muted); margin: 0 0 1.5rem; }
  .controls { display: flex; flex-wrap: wrap; gap: 0.75rem; margin-bottom: 0.5rem; }
  .controls input, .controls select {
    padding: 0.6rem 0.8rem; font-size: 1rem; border-radius: 8px;
    border: 1px solid var(--border); background: var(--card); color: var(--text);
  }
  .controls input { flex: 1; min-width: 200px; }
  #counter { color: var(--muted); font-size: 0.9rem; margin: 0 0 1rem; }
  .card {
    background: var(--card); border: 1px solid var(--border);
    border-radius: 12px; padding: 1.25rem; margin-bottom: 1rem;
  }
  .card h2 { font-size: 1.15rem; margin: 0.4rem 0 0.5rem; }
  .category {
    font-size: 0.8rem; font-weight: 600; color: var(--accent);
    text-transform: uppercase; letter-spacing: 0.03em;
  }
  .summary { margin: 0 0 0.75rem; }
  .keywords { display: flex; flex-wrap: wrap; gap: 0.4rem; }
  .keyword { background: var(--chip); border-radius: 999px; padding: 0.15rem 0.6rem; font-size: 0.85rem; }
  .filename { color: var(--muted); font-size: 0.85rem; margin: 0.75rem 0 0; }
  .filename a { color: inherit; }
  [hidden] { display: none !important; }
</style>
</head>
<body>
<main>
  <h1>Research Library</h1>
  <p class="meta">$count papers &middot; last updated $updated</p>

  <div class="controls">
    <input id="search" type="search" placeholder="Search titles, summaries, keywords...">
    <select id="category">
      <option value="">All categories</option>
      $options
    </select>
  </div>
  <p id="counter"></p>

  <section id="papers">
    $cards
  </section>
  <p id="empty" hidden>No papers to show.</p>
</main>

<script>
  const search = document.getElementById("search");
  const category = document.getElementById("category");
  const cards = document.querySelectorAll(".card");
  const counter = document.getElementById("counter");
  const empty = document.getElementById("empty");

  function applyFilters() {
    const query = search.value.trim().toLowerCase();
    const selected = category.value;
    let visible = 0;

    cards.forEach(function (card) {
      const matchesText = card.dataset.search.includes(query);
      const matchesCategory = selected === "" || card.dataset.category === selected;
      const show = matchesText && matchesCategory;
      card.hidden = !show;
      if (show) visible++;
    });

    counter.textContent = "Showing " + visible + " of " + cards.length + " papers";
    empty.hidden = visible !== 0;
  }

  search.addEventListener("input", applyFilters);
  category.addEventListener("change", applyFilters);
  applyFilters();
</script>
</body>
</html>
""")


def _paper_card(paper: dict) -> str:
    """Return the HTML for one paper. All LLM-generated text is escaped."""
    title = html.escape(paper["title"])
    summary = html.escape(paper["summary"])
    category = html.escape(paper["category"])
    keywords = "".join(
        f'<span class="keyword">{html.escape(keyword)}</span>' for keyword in paper["keywords"]
    )

    # Lowercased text the search box matches against: title + summary + keywords
    search_text = " ".join([paper["title"], paper["summary"], *paper["keywords"]]).lower()

    # Relative link: works when index.html (in output/) and the PDFs (in papers/) are synced side by side
    pdf_link = "../papers/" + quote(paper["filename"])

    return f"""
    <article class="card" data-category="{category}" data-search="{html.escape(search_text)}">
      <span class="category">{category}</span>
      <h2>{title}</h2>
      <p class="summary">{summary}</p>
      <div class="keywords">{keywords}</div>
      <p class="filename"><a href="{pdf_link}">{html.escape(paper["filename"])}</a></p>
    </article>"""


def build_html(papers: list[dict], output_path: str | Path) -> None:
    """Write the full library page to output_path, replacing the previous version."""
    output_path = Path(output_path)
    papers = sorted(papers, key=lambda paper: paper["title"].lower())
    categories = sorted({paper["category"] for paper in papers})

    page = PAGE_TEMPLATE.substitute(
        count=len(papers),
        updated=datetime.now().strftime("%Y-%m-%d %H:%M"),
        options="".join(
            f'<option value="{html.escape(c)}">{html.escape(c)}</option>' for c in categories
        ),
        cards="".join(_paper_card(paper) for paper in papers),
    )

    # Write to a temporary file, then rename it into place in one step,
    # so Google Drive never syncs a half-written page.
    output_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = output_path.with_name(output_path.name + ".tmp")
    tmp_path.write_text(page, encoding="utf-8")
    os.replace(tmp_path, output_path)


if __name__ == "__main__":
    # Quick manual test without Airflow: python dags/html_page.py
    from config import HTML_PATH
    from db import get_all_papers

    build_html(get_all_papers(), HTML_PATH)
    print(f"Wrote {HTML_PATH}")