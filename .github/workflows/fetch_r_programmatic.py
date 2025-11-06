# fetch_r_programmatic.py
import json, os, time, sys
import praw

REDDIT_CLIENT_ID = os.environ.get("REDDIT_CLIENT_ID")
REDDIT_CLIENT_SECRET = os.environ.get("REDDIT_CLIENT_SECRET")
REDDIT_USER_AGENT = "r-programmatic-json-updater by u/<yourname>"

SUBREDDIT = "programmatic"
LIMIT = 500          # bump this up to pull more (PRAW will page for you)
SORT = "new"         # "new", "hot", or "top"
OUTFILE = "r_programmatic.json"

def post_to_dict(p):
    return {
        "id": p.id,
        "title": p.title,
        "author": str(p.author) if p.author else None,
        "created_utc": p.created_utc,
        "score": p.score,
        "num_comments": p.num_comments,
        "url": p.url,
        "permalink": f"https://www.reddit.com{p.permalink}",
        "selftext": p.selftext,
        "over_18": p.over_18,
        "link_flair_text": p.link_flair_text,
    }

def main():
    if not REDDIT_CLIENT_ID or not REDDIT_CLIENT_SECRET:
        print("ERROR: Missing Reddit credentials in env", file=sys.stderr)
        sys.exit(2)

    reddit = praw.Reddit(
        client_id=REDDIT_CLIENT_ID,
        client_secret=REDDIT_CLIENT_SECRET,
        user_agent=REDDIT_USER_AGENT,
    )
    reddit.read_only = True

    sub = reddit.subreddit(SUBREDDIT)
    if SORT == "new":
        it = sub.new(limit=LIMIT)
    elif SORT == "top":
        it = sub.top(limit=LIMIT)
    else:
        it = sub.hot(limit=LIMIT)

    items = []
    for i, p in enumerate(it, 1):
        items.append(post_to_dict(p))
        if i % 100 == 0:
            print(f"...fetched {i} posts")

    snapshot = {
        "subreddit": SUBREDDIT,
        "fetched_at_utc": int(time.time()),
        "count": len(items),
        "items": items,
    }

    with open(OUTFILE, "w", encoding="utf-8") as f:
        json.dump(snapshot, f, ensure_ascii=False, indent=2)

    print(f"✅ Saved {len(items)} posts to {OUTFILE}")

    # Fail the job if we saved nothing, so it's obvious
    if len(items) == 0:
        print("ERROR: 0 posts saved — check API creds/permissions/rate limits", file=sys.stderr)
        sys.exit(3)

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(1)
