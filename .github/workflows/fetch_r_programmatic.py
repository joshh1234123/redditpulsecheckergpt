# fetch_r_programmatic.py
import json, os, time
import praw

REDDIT_CLIENT_ID = os.environ["REDDIT_CLIENT_ID"]
REDDIT_CLIENT_SECRET = os.environ["REDDIT_CLIENT_SECRET"]
REDDIT_USER_AGENT = "r-programmatic-json-updater by u/<yourname>"

# Tweak these
SUBREDDIT = "programmatic"
LIMIT = 100          # how many items per run
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
    reddit = praw.Reddit(
        client_id=REDDIT_CLIENT_ID,
        client_secret=REDDIT_CLIENT_SECRET,
        user_agent=REDDIT_USER_AGENT,
    )

    sub = reddit.subreddit(SUBREDDIT)
    if SORT == "new":
        it = sub.new(limit=LIMIT)
    elif SORT == "top":
        it = sub.top(limit=LIMIT)
    else:
        it = sub.hot(limit=LIMIT)

    items = [post_to_dict(p) for p in it]
    snapshot = {
        "subreddit": SUBREDDIT,
        "fetched_at_utc": int(time.time()),
        "count": len(items),
        "items": items,
    }

    with open(OUTFILE, "w", encoding="utf-8") as f:
        json.dump(snapshot, f, ensure_ascii=False, indent=2)

if __name__ == "__main__":
    main()
