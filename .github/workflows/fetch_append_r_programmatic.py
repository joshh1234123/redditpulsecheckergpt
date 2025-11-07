# .github/workflows/fetch_append_r_programmatic.py
# Appends new r/programmatic posts (with full comments) to a rolling JSON DB.
# Robust against a corrupted/partially edited existing DB: tries to repair, else backs it up and continues.
# Requires env: REDDIT_CLIENT_ID, REDDIT_CLIENT_SECRET
# Writes: programmatic_complete_for_llm.json in repo root

import os, json, datetime, re
from pathlib import Path
from typing import Dict, Any, List
import praw

SUBREDDIT = "programmatic"
OUTPUT_PATH = Path("programmatic_complete_for_llm.json")
MAX_NEW_TO_PULL = 1000
FETCH_COMMENTS = True
USER_AGENT = "reddit-r-programmatic-archiver/1.1 (by u/yourbot)"

def now_iso() -> str:
    return datetime.datetime.now(datetime.timezone.utc).isoformat()

def empty_db() -> Dict[str, Any]:
    return {
        "metadata": {
            "source": f"Reddit r/{SUBREDDIT}",
            "scrape_date": now_iso(),
            "total_posts": 0,
            "total_comments": 0
        },
        "discussions": []
    }

# --- tolerant JSON loader helpers ---
def _strip_bom(s: str) -> str:
    return s.lstrip("\ufeff")

def _remove_merge_markers(s: str) -> str:
    # remove any Git conflict blocks
    return re.sub(r"<<<<<<<.*?>>>>>>>\s*", "", s, flags=re.DOTALL)

def _remove_inline_comments(s: str) -> str:
    # remove // comments (naive, line-based) – safe enough for rescue attempts
    s = re.sub(r"^\s*//.*?$", "", s, flags=re.MULTILINE)
    # remove /* ... */ blocks (naive)
    s = re.sub(r"/\*.*?\*/", "", s, flags=re.DOTALL)
    return s

def _strip_to_outer_braces(s: str) -> str:
    # keep from first { to last }
    start = s.find("{")
    end = s.rfind("}")
    if start != -1 and end != -1 and end > start:
        return s[start:end+1]
    return s

def _fix_trailing_commas(s: str) -> str:
    # remove trailing commas before } or ]
    return re.sub(r",\s*([}\]])", r"\1", s)

def try_parse_relaxed(txt: str):
    candidates = []
    s = _strip_bom(txt)
    candidates.append(s)
    s = _remove_merge_markers(s)
    candidates.append(s)
    s = _remove_inline_comments(s)
    candidates.append(s)
    s = _strip_to_outer_braces(s)
    candidates.append(s)
    s = _fix_trailing_commas(s)
    candidates.append(s)
    # try each progressive transform
    last_err = None
    for c in candidates:
        try:
            return json.loads(c)
        except Exception as e:
            last_err = e
    raise last_err if last_err else ValueError("Unknown JSON parse error")

def load_db(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return empty_db()
    raw = path.read_text(encoding="utf-8", errors="replace")
    try:
        data = json.loads(raw)
    except Exception:
        # attempt relaxed repair
        try:
            print("[warn] DB JSON invalid. Attempting light repair…")
            data = try_parse_relaxed(raw)
            print("[warn] Repair succeeded.")
        except Exception as e:
            # back up and start fresh so the run continues
            backup = path.with_suffix(".corrupt.backup.json")
            backup.write_text(raw, encoding="utf-8", errors="ignore")
            print(f"[error] DB is not valid JSON (saved backup to {backup.name}). Starting from empty DB. Error: {e}")
            return empty_db()

    # normalize shape
    if "discussions" in data:
        data.setdefault("metadata", {"source": f"Reddit r/{SUBREDDIT}"})
        return data
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
    tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(path)

def post_key_from_existing(d: Dict[str, Any]) -> str:
    pid = d.get("id")
    if pid:
        return f"id:{pid}"
    return f"url:{d.get('url','')}|ts:{d.get('created_utc','')}"

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
            print(f"[warn] Comment fetch error on {submission.id}: {e}")
            record["comments"] = []
    else:
        record["comments"] = []
    return record

def main():
    cid = os.environ.get("REDDIT_CLIENT_ID")
    csec = os.environ.get("REDDIT_CLIENT_SECRET")
    if not cid or not csec:
        raise SystemExit("Missing REDDIT_CLIENT_ID or REDDIT_CLIENT_SECRET")

    reddit = praw.Reddit(client_id=cid, client_secret=csec, user_agent=USER_AGENT)
    reddit.read_only = True

    db = load_db(OUTPUT_PATH)
    discussions: List[Dict[str, Any]] = db.get("discussions", [])

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

    for submission in sub.new(limit=MAX_NEW_TO_PULL):
        fetched += 1
        created = float(getattr(submission, "created_utc", 0.0) or 0.0)
        if cutoff and created <= cutoff:
            break
        key = f"id:{submission.id}" if submission.id else f"url:{submission.url}|ts:{created}"
        if key in seen:
            continue
        rec = serialize_submission(submission)
        new_items.append(rec)
        seen.add(key)
        if len(new_items) % 50 == 0:
            print(f"...collected {len(new_items)} new posts so far (fetched {fetched})")

    if not new_items:
        print(f"No new items found. Fetched {fetched} posts from r/{SUBREDDIT}.")
        save_db(OUTPUT_PATH, db)   # update metadata scrape_date so you can see the run occurred
        return

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
        import traceback
        traceback.print_exc()
        raise
