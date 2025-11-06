# .github/workflows/fetch_append_r_programmatic.py
# Appends new r/programmatic posts (with full comments) to an existing rolling JSON DB.
# Expects env vars: REDDIT_CLIENT_ID, REDDIT_CLIENT_SECRET
# Writes in repo root: programmatic_complete_for_llm.json

import os
import json
import time
import datetime
from pathlib import Path
from typing import Dict, Any, List

import praw


# ---------- Config ----------
SUBREDDIT = "programmatic"
OUTPUT_PATH = Path("programmatic_complete_for_llm.json")
MAX_NEW_TO_PULL = 1000          # safety cap per run
FETCH_COMMENTS = True           # set False to skip comment bodies for speed
USER_AGENT = "reddit-r-programmatic-archiver/1.0 (by u/yourbot)"


# ---------- Helpers ----------
def now_iso() -> str:
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


def load_db(path: Path) -> Dict[str, Any]:
    """
    Supports two shapes:
      A) {"metadata": {...}, "discussions": [post, ...]}
      B) {"subreddit": "...", "items": [post, ...], "count": N}  (older simple schema)
    Normalizes to {"metadata": {...}, "discussions": [...]}
    """
    if not path.exists():
        return {
            "metadata": {
                "source": f"Reddit r/{SUBREDDIT}",
                "scrape_date": now_iso(),
                "total_posts": 0,
                "total_comments": 0
            },
            "discussions": []
        }

    with path.open("r", encoding="utf-8") as f:
        data = json.load(f)

    if "discussions" in data:
        # Already in the preferred shape
        if "metadata" not in data:
            data["metadata"] = {"source": f"Reddit r/{SUBREDDIT}"}
        return data

    # Simple schema normalization
    discussions = data.get("items", [])
    meta = {
        "source": f"Reddit r/{data.get('subreddit', SUBREDDIT)}",
        "scrape_date": now_iso(),
        "total_posts": len(discussions),
        "total_comments": sum(len(d.get("comments", [])) for d in discussions if isinstance(d, dict)),
    }
    return {"metadata": meta, "discussions": discussions}


def save_db(path: Path, data: Dict[str, Any]) -> None:
    total_posts = len(data.get("discussions", []))
    total_comments = sum(len(d.get("comments", [])) for d in data.get("discussions", []))
    data.setdefault("metadata", {})
    data["metadata"]["source"] = f"Reddit r/{SUBREDDIT}"
    data["metadata"]["scrape_date"] = now_iso()
    data["metadata"]["total_posts"] = total_posts
    data["metadata"]["total_comments"] = total_comments

    tmp = path.with_suffix(".tmp.json")
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    tmp.replace(path)


def post_key_from_existing(d: Dict[str, Any]) -> str:
    # Stable key for dedupe across schema variations
    pid = d.get("id")
    if pid:
        return f"id:{pid}"
    url = d.get("url", "")
    ts = d.get("created_utc", "")
    return f"url:{url}|ts:{ts}"


def serialize_submission(submission) -> Dict[str, Any]:
    created = float(getattr(submission, "created_utc", 0.0) or 0.0)
    record: Dict[str, Any] = {
        "id": submission.id,
        "title": submission.title,
        "author": getattr(submission.author, "name", None),
        "score": int(getattr(submission, "score", 0) or 0),
        "url": submission.url,
        "permalink": f"https://www.reddit.com{submission.permalink}",
        "created_utc": created,
        "content": submission.selftext or "",
        "num_comments": int(getattr(submission, "num_comments", 0) or 0),
        "over_18": bool(getattr(submission, "over_18", False)),
        "link_flair_text": getattr(submission, "link_flair_text", None),
    }

    if FETCH_COMMENTS:
        try:
            submission.comments.replace_more(limit=0)
            comments: List[Dict[str, Any]] = []
            for c in submission.comments.list():
                comments.append({
                    "author": getattr(c.author, "name", None),
                    "body": c.body,
                    "score": int(getattr(c, "score", 0) or 0),
                    "created_utc": float(getattr(c, "created_utc", 0.0) or 0.0),
                })
            record["comments"] = comments
        except Exception as e:
            print(f"Comment fetch error on {submission.id}: {e}")
            record["comments"] = []
    else:
        record["comments"] = []

    return record


# ---------- Main ----------
def main():
    cid = os.environ.get("REDDIT_CLIENT_ID")
    csec = os.environ.get("REDDIT_CLIENT_SECRET")
    if not cid or not csec:
        raise SystemExit("Missing REDDIT_CLIENT_ID or REDDIT_CLIENT_SECRET")

    reddit = praw.Reddit(
        client_id=cid,
        client_secret=csec,
        user_agent=USER_AGENT
    )
    reddit.read_only = True

    # Load current DB
    db = load_db(OUTPUT_PATH)
    discussions: List[Dict[str, Any]] = db.get("discussions", [])

    # Build dedupe set and find cutoff timestamp
    seen = set(post_key_from_existing(d) for d in discussions)
    cutoff = 0.0
    for d in discussions:
        try:
            ts = float(d.get("created_utc", 0.0) or 0.0)
            if ts > cutoff:
                cutoff = ts
        except Exception:
            pass

    print(f"Existing posts: {len(discussions)}")
    if cutoff:
        print(f"Cutoff created_utc: {cutoff} ({datetime.datetime.utcfromtimestamp(cutoff)})")

    sub = reddit.subreddit(SUBREDDIT)

    new_items: List[Dict[str, Any]] = []
    fetched = 0

    # Pull newest first and stop when we reach or pass the cutoff
    for submission in sub.new(limit=MAX_NEW_TO_PULL):
        fetched += 1
        created = float(getattr(submission, "created_utc", 0.0) or 0.0)

        if cutoff and created <= cutoff:
            # We reached content that is not newer than what we already have
            break

        key = f"id:{submission.id}" if submission.id else f"url:{submission.url}|ts:{created}"
        if key in seen:
            continue

        rec = serialize_submission(submission)
        new_items.append(rec)
        seen.add(key)

        # Progress log on larger pulls
        if len(new_items) % 50 == 0:
            print(f"...collected {len(new_items)} new posts so far (fetched {fetched})")

    if not new_items:
        print(f"No new items found. Fetched {fetched} posts from r/{SUBREDDIT}.")
        # Still bump metadata scrape_date so you can see the run occurred
        save_db(OUTPUT_PATH, db)
        return

    # Merge and sort by created_utc ascending for consistent history
    discussions.extend(new_items)
    discussions.sort(key=lambda d: float(d.get("created_utc", 0.0) or 0.0))
    db["discussions"] = discussions

    save_db(OUTPUT_PATH, db)

    print(f"Added {len(new_items)} new posts. Total now {len(discussions)}.")
    newest = discussions[-1]["created_utc"]
    print(f"Newest post time: {newest} ({datetime.datetime.utcfromtimestamp(newest)})")
    total_comments = sum(len(d.get("comments", [])) for d in discussions)
    print(f"Total comments stored: {total_comments}")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        # Fail loudly so the workflow shows red when something breaks
        import traceback
        traceback.print_exc()
        raise
