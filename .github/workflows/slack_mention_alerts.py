#!/usr/bin/env python3
"""Scan the rolling r/programmatic DB for new posts/comments mentioning a keyword
(default: "stackadapt") and send Slack alerts via an incoming webhook.

State of already-alerted IDs is kept in automation_state/mentioned_seen.json so
each match is only announced once. Designed to run after the fetch step in the
existing workflow.

Env vars:
  SLACK_WEBHOOK_URL   required, Slack incoming webhook
  MENTION_KEYWORDS    optional, comma-separated; defaults to "stackadapt"
  DB_FILE             optional, path to rolling DB; defaults to
                      programmatic_complete_for_llm.json
"""
import hashlib
import json
import os
import sys
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

DB_FILE = Path(os.environ.get("DB_FILE", "programmatic_complete_for_llm.json"))
STATE_FILE = Path("automation_state/mentioned_seen.json")
KEYWORDS = [
    k.strip().lower()
    for k in os.environ.get("MENTION_KEYWORDS", "stackadapt").split(",")
    if k.strip()
]


def load_state() -> dict:
    if STATE_FILE.exists():
        try:
            data = json.loads(STATE_FILE.read_text())
            if isinstance(data, dict) and "posts" in data and "comments" in data:
                return data
        except json.JSONDecodeError:
            pass
    return {"posts": [], "comments": []}


def save_state(state: dict) -> None:
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    state["posts"] = sorted(set(state["posts"]))[-5000:]
    state["comments"] = sorted(set(state["comments"]))[-20000:]
    STATE_FILE.write_text(json.dumps(state, indent=2))


def matches(text: str) -> bool:
    t = (text or "").lower()
    return any(k in t for k in KEYWORDS)


def iter_comments(node):
    """Yield every comment dict, walking nested replies if present."""
    if isinstance(node, list):
        for c in node:
            yield from iter_comments(c)
        return
    if isinstance(node, dict):
        if "body" in node or "id" in node:
            yield node
        for key in ("replies", "comments", "children"):
            if key in node:
                yield from iter_comments(node[key])


def ts(post) -> str:
    try:
        return datetime.fromtimestamp(
            float(post.get("created_utc", 0)), tz=timezone.utc
        ).strftime("%Y-%m-%d %H:%M UTC")
    except Exception:
        return ""


def post_url(post) -> str:
    p = post.get("permalink") or post.get("url") or ""
    if p.startswith("/"):
        p = f"https://www.reddit.com{p}"
    return p


def slack_post(webhook: str, payload: dict) -> None:
    req = urllib.request.Request(
        webhook,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=20) as resp:
        if resp.status >= 300:
            raise RuntimeError(f"Slack webhook returned {resp.status}")


def truncate(text: str, n: int = 500) -> str:
    text = (text or "").strip()
    return text if len(text) <= n else text[: n - 1].rstrip() + "…"


def build_post_message(post, hit_field: str) -> dict:
    title = post.get("title") or "(no title)"
    body = truncate(post.get("content") or post.get("selftext") or "")
    kw = ", ".join(KEYWORDS)
    return {
        "text": f":mega: New r/programmatic *post* mentions *{kw}*: {title}",
        "blocks": [
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": (
                        f":mega: *New r/programmatic post mentions "
                        f"`{kw}`* (in {hit_field})\n"
                        f"*<{post_url(post)}|{title}>*\n"
                        f"_by u/{post.get('author', '?')} · {ts(post)}_"
                    ),
                },
            },
            {
                "type": "section",
                "text": {"type": "mrkdwn", "text": body or "_(no body)_"},
            },
        ],
    }


def build_comment_message(post, comment) -> dict:
    title = post.get("title") or "(no title)"
    body = truncate(comment.get("body") or "")
    permalink = comment.get("permalink") or post_url(post)
    if permalink.startswith("/"):
        permalink = f"https://www.reddit.com{permalink}"
    kw = ", ".join(KEYWORDS)
    return {
        "text": f":speech_balloon: New r/programmatic *comment* mentions *{kw}*",
        "blocks": [
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": (
                        f":speech_balloon: *New r/programmatic comment "
                        f"mentions `{kw}`*\n"
                        f"on *<{post_url(post)}|{title}>*\n"
                        f"_by u/{comment.get('author', '?')}_ "
                        f"<{permalink}|view comment>"
                    ),
                },
            },
            {
                "type": "section",
                "text": {"type": "mrkdwn", "text": body or "_(no body)_"},
            },
        ],
    }


def main() -> int:
    webhook = os.environ.get("SLACK_WEBHOOK_URL")
    if not webhook:
        print("SLACK_WEBHOOK_URL not set; skipping mention alerts.")
        return 0
    if not DB_FILE.exists():
        print(f"DB file {DB_FILE} not found; nothing to scan.")
        return 0

    db = json.loads(DB_FILE.read_text())
    posts = db.get("discussions") or db.get("items") or []
    state = load_state()
    seen_posts = set(state["posts"])
    seen_comments = set(state["comments"])

    new_post_alerts = 0
    new_comment_alerts = 0

    for post in posts:
        pid = post.get("id")
        title = post.get("title") or ""
        body = post.get("content") or post.get("selftext") or ""
        if pid and pid not in seen_posts:
            hit_field = None
            if matches(title):
                hit_field = "title"
            elif matches(body):
                hit_field = "body"
            if hit_field:
                try:
                    slack_post(webhook, build_post_message(post, hit_field))
                    new_post_alerts += 1
                except Exception as e:
                    print(f"Slack post alert failed for {pid}: {e}", file=sys.stderr)
                    continue
            seen_posts.add(pid)

        for c in iter_comments(post.get("comments") or []):
            cid = c.get("id") or hashlib.sha1(
                "|".join(
                    [
                        str(pid or ""),
                        str(c.get("author", "")),
                        str(c.get("created_utc", "")),
                        (c.get("body") or "")[:120],
                    ]
                ).encode("utf-8")
            ).hexdigest()
            if cid in seen_comments:
                continue
            if matches(c.get("body") or ""):
                try:
                    slack_post(webhook, build_comment_message(post, c))
                    new_comment_alerts += 1
                except Exception as e:
                    print(f"Slack comment alert failed for {cid}: {e}", file=sys.stderr)
                    continue
            seen_comments.add(cid)

    state["posts"] = list(seen_posts)
    state["comments"] = list(seen_comments)
    save_state(state)
    print(
        f"Mention scan done. New post alerts: {new_post_alerts}, "
        f"new comment alerts: {new_comment_alerts}, keywords: {KEYWORDS}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
