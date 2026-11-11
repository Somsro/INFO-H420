import time
import requests
from config import DISCORD_WEBHOOK_URL

def send_discord_webhook(papers: list[dict]) -> None:
  """Post rich embed cards to Discord in batches of 10 to respect Discord's API payload limit."""

  if not DISCORD_WEBHOOK_URL or not papers:
    print("No Discord Webhook URL found or no new papers to notify.")
    return

  BATCH_SIZE = 10
  total_papers = len(papers)
  total_batches = (total_papers + BATCH_SIZE - 1) // BATCH_SIZE

  # Slice papers into chunks of 10
  for i in range(0, total_papers, BATCH_SIZE):
    batch = papers[i : i + BATCH_SIZE]
    batch_num = (i // BATCH_SIZE) + 1

    embeds = []
    for paper in batch:
      embeds.append({
          "title": f"📄 {paper.get('title', paper.get('filename'))}",
          "color": 3447003,  # Blue accent
          "fields": [
              {
                  "name": "Category",
                  "value": paper.get("category", "Uncategorized"),
                  "inline": True,
              },
              {
                  "name": "Keywords",
                  "value": ", ".join(paper.get("keywords", [])) or "None",
                  "inline": True,
              },
              {
                  "name": "Summary",
                  "value": paper.get("summary", "No summary available.")[
                      :1000
                  ],
                  "inline": False,
              },
          ],
      })

    payload = {
        "username": "Paper Pipeline Bot",
        "content": (
            f"**Processed {total_papers} paper(s) — Batch"
            f" {batch_num}/{total_batches}:**"
        ),
        "embeds": embeds,
    }

    response = requests.post(DISCORD_WEBHOOK_URL, json=payload, timeout=10)
    response.raise_for_status()

    # Small 0.5s pause between API calls to stay within Discord's rate limit (5 reqs/sec)
    if i + BATCH_SIZE < total_papers:
        time.sleep(0.5)