import os, json, time, datetime
from pathlib import Path

import praw

SUBREDDIT = "programmatic"
OUTPUT_PATH = "programmatic_complete_for_llm.json"  # your existing DB
MAX_NEW_TO_PULL = 1000  # safety cap per run

def now_iso():
    return datetime.datetime.now(datetime.timezone.utc).isoformat()

def load_db(path: Path):
    if not path.exists():
        return {"metadata": {"source": f"Reddit r/{SUBREDDIT}",
                             "scrape_date": now_iso(),
                             "total_posts": 0,
                             "total_comments": 0},
                "discussions": []}
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)

def save_db(path: Path, data: dict):
    # refresh metadata counts
    total_posts = len(data.get("discussions", []))
    total_comments = sum(len(d.get("comments", [])) for d in data.get("discussions", []))
    data["metadata"]["scrape_date"] = now_iso()
    data["metadata"]["total_posts"] = total_posts
    data["metadata"]["total_comments"] = total_comments

    tmp = path.with_suffix(".tmp.json")
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    tmp.replace(path)

def main():
    cid = os.environ.get("REDDIT_CLIENT_ID")
    csec = os.environ.get("REDDIT_CLIENT_SECRET")
    if not cid or not csec:
        raise SystemExit("Missing REDDIT_CLIENT_ID or REDDIT_CLIENT_SECRET")

    reddit = praw.Reddit(
        client_id=cid,
        client_secret=csec,
        user_agent="reddit-r-programmatic-archiver/1.0 (by u/yourbot)"
    )

    out_path = Path(OUTPUT_PATH)
    db = load_db(out_path)

    discussions = db.get("discussions", [])
    # dedupe set by submission id if present, else url+created_utc
    seen_ids = set()
    for d in discussions:
        # Original DB may not have 'id'. We derive a synthetic key when missing.
        key = d.get("id") or f"{d.get('url','')}|{d.get('created_utc','')}"
        seen_ids.add(key)

    # find cutoff: latest created_utc in existing DB
    cutoff = 0.0
    for d in discussions:
        try:
            ts = float(d.get("created_utc", 0.0))
            if ts > cutoff:
                cutoff = ts
        except Exception:
            pass

    print(f"Existing posts: {len(discussions)}")
    if cutoff:
        print(f"Cutoff created_utc: {cutoff} ({datetime.datetime.utcfromtimestamp(cutoff)})")

    sub = reddit.subreddit(SUBREDDIT)

    new_items = []
    fetched = 0
    for idx, submission in enumerate(sub.new(limit=MAX_NEW_TO_PULL)):
        fetched += 1
        created = float(submission.created_utc or 0.0)
        # Stop when we reach or pass the last known item’s time
        if cutoff and created <= cutoff:
            break

        # Build a stable key
        key = submission.id or f"{submission.url}|{created}"
        if key in seen_ids:
            continue

        # Pull comments for this submission
        try:
            submission.comments.replace_more(limit=0)
            comments = []
            for c in submission.comments.list():
                comments.append({
                    "author": getattr(c.author, "name", None),
                    "body": c.body,
                    "score": int(getattr(c, "score", 0) or 0),
                    "created_utc": float(getattr(c, "created_utc", 0.0) or 0.0),
                })
        except Exception as e:
            print(f"Comment fetch error on {submission.id}: {e}")
            comments = []

        record = {
            "id": submission.id,
            "title": submission.title,
            "author": getattr(submission.author, "name", None),
            "score": int(getattr(submission, "score", 0) or 0),
            "url": submission.url,
            "created_utc": created,
            "content": submission.selftext or "",
            "num_comments": int(getattr(submission, "num_comments", 0) or 0),
            "comments": comments,
        }
        new_items.append(record)
        seen_ids.add(key)

    if not new_items:
        print(f"No new items found. Fetched {fetched} posts from r/{SUBREDDIT}.")
        return

    # Merge, sort by time ascending to preserve history order
    discussions.extend(new_items)
    discussions.sort(key=lambda d: float(d.get("created_utc", 0.0)))

    db["discussions"] = discussions
    if "metadata" not in db:
        db["metadata"] = {"source": f"Reddit r/{SUBREDDIT}"}
    db["metadata"]["source"] = f"Reddit r/{SUBREDDIT}"

    save_db(out_path, db)

    print(f"Added {len(new_items)} new posts. Total now {len(discussions)}.")
    newest = discussions[-1]["created_utc"] if discussions else 0
    print(f"Newest post time: {newest} ({datetime.datetime.utcfromtimestamp(newest)})")

if __name__ == "__main__":
    main()
