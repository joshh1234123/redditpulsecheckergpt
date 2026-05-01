#!/usr/bin/env python3
"""Weekly sentiment + topic check for r/programmatic.

Aggregates all posts (and their comments) created in the last 7 days from the
rolling DB, computes:
  * lexicon-based sentiment score per post and overall
  * top topics (frequent meaningful keywords + bigrams)
  * a few example posts per sentiment bucket

Sends a digest to Slack via SLACK_WEBHOOK_URL.

Env vars:
  SLACK_WEBHOOK_URL   required
  DB_FILE             optional, defaults to programmatic_complete_for_llm.json
  LOOKBACK_DAYS       optional, defaults to 7
"""
import json
import os
import re
import sys
import urllib.request
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path

DB_FILE = Path(os.environ.get("DB_FILE", "programmatic_complete_for_llm.json"))
LOOKBACK_DAYS = int(os.environ.get("LOOKBACK_DAYS", "7"))

POSITIVE = {
    "great", "love", "loving", "awesome", "amazing", "excellent", "good",
    "happy", "excited", "win", "winning", "win-win", "easy", "smooth",
    "useful", "helpful", "fantastic", "solid", "best", "better", "improved",
    "improvement", "wins", "fast", "reliable", "transparent", "fair",
    "recommend", "recommended", "promising", "encouraging", "positive",
    "profitable", "scalable", "efficient", "value",
}
NEGATIVE = {
    "bad", "worse", "worst", "hate", "terrible", "awful", "horrible",
    "frustrating", "frustrated", "annoying", "annoyed", "broken", "buggy",
    "slow", "expensive", "scam", "shady", "fraud", "fraudulent", "useless",
    "garbage", "trash", "fail", "failed", "failing", "problem", "problems",
    "issue", "issues", "concerns", "concerned", "disappointed",
    "disappointing", "confusing", "confused", "complicated", "opaque",
    "overpriced", "wasted", "waste", "stuck", "lost", "regret", "decline",
    "declining", "tired", "burned", "burnt", "downturn", "layoff", "layoffs",
}

STOPWORDS = {
    "the", "a", "an", "and", "or", "but", "if", "then", "of", "to", "in",
    "on", "for", "with", "by", "is", "are", "was", "were", "be", "been",
    "being", "this", "that", "these", "those", "it", "its", "as", "at",
    "from", "we", "you", "i", "they", "them", "our", "your", "their", "my",
    "me", "us", "he", "she", "his", "her", "do", "does", "did", "doing",
    "have", "has", "had", "having", "not", "no", "yes", "so", "also", "just",
    "than", "too", "very", "much", "more", "most", "less", "least", "some",
    "any", "all", "every", "each", "either", "neither", "into", "out", "up",
    "down", "off", "over", "under", "again", "further", "once", "here",
    "there", "when", "where", "why", "how", "what", "which", "who", "whom",
    "can", "cant", "could", "should", "would", "will", "wont", "may", "might",
    "must", "shall", "about", "after", "before", "because", "while", "during",
    "between", "through", "etc", "ie", "eg", "vs", "via", "im", "ive", "id",
    "youre", "theyre", "theres", "thats", "doesnt", "didnt", "isnt", "arent",
    "wasnt", "werent", "havent", "hasnt", "wouldnt", "shouldnt", "couldnt",
    "really", "actually", "anyone", "anything", "something", "nothing",
    "someone", "everyone", "lot", "lots", "thing", "things", "way", "ways",
    "use", "used", "using", "get", "got", "getting", "make", "made", "making",
    "go", "going", "went", "come", "came", "see", "saw", "seen", "know",
    "known", "think", "thought", "want", "wants", "wanted", "need", "needs",
    "needed", "look", "looks", "looking", "say", "said", "says", "tell",
    "told", "ask", "asked", "good", "bad", "new", "old", "now", "still",
    "even", "ever", "yet", "back", "around", "across", "people", "guys",
    "anyone's", "let", "lets", "post", "posts", "comment", "comments",
    "thread", "subreddit", "reddit",
}

# Marketing/programmatic-specific topic vocabulary that should pass the
# stopword filter even if short.
TOPIC_BOOST = {
    "ctv", "cdp", "dmp", "dsp", "ssp", "ad", "ads", "adtech", "ai", "agentic",
    "rtb", "ttd", "dv360", "gam", "ga4", "gtm", "cpa", "cpm", "cpc", "cpl",
    "roas", "lift", "incrementality", "attribution", "fraud", "ivt",
    "audience", "audiences", "cookies", "cookieless", "id", "ids", "identity",
    "first-party", "third-party", "retargeting", "prospecting", "creatives",
    "creative", "targeting", "lookalike", "video", "display", "native",
    "audio", "podcast", "dooh", "ooh", "openai", "chatgpt", "gemini",
    "perplexity", "meta", "google", "amazon", "tiktok", "linkedin",
    "stackadapt", "criteo", "trade desk", "the trade desk", "viant",
    "xandr", "magnite", "pubmatic", "supply path",
}

WORD_RE = re.compile(r"[A-Za-z][A-Za-z0-9'\-]+")


def load_db():
    return json.loads(DB_FILE.read_text())


def iter_comments(node):
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


def tokenize(text: str):
    return [w.lower() for w in WORD_RE.findall(text or "")]


def is_topic_token(tok: str) -> bool:
    if tok in TOPIC_BOOST:
        return True
    if tok in STOPWORDS:
        return False
    if len(tok) < 4:
        return False
    if tok.isdigit():
        return False
    return True


def sentiment_score(tokens):
    pos = sum(1 for t in tokens if t in POSITIVE)
    neg = sum(1 for t in tokens if t in NEGATIVE)
    total = pos + neg
    if total == 0:
        return 0.0, pos, neg
    return (pos - neg) / total, pos, neg


def label(score: float, total_hits: int) -> str:
    if total_hits == 0:
        return "neutral"
    if score >= 0.25:
        return "positive"
    if score <= -0.25:
        return "negative"
    return "mixed"


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


def main() -> int:
    webhook = os.environ.get("SLACK_WEBHOOK_URL")
    if not webhook:
        print("SLACK_WEBHOOK_URL not set; skipping weekly sentiment.")
        return 0
    if not DB_FILE.exists():
        print(f"DB file {DB_FILE} not found.")
        return 0

    db = load_db()
    posts = db.get("discussions") or db.get("items") or []
    cutoff = datetime.now(timezone.utc) - timedelta(days=LOOKBACK_DAYS)
    cutoff_ts = cutoff.timestamp()

    recent = []
    for p in posts:
        try:
            if float(p.get("created_utc", 0)) >= cutoff_ts:
                recent.append(p)
        except (TypeError, ValueError):
            continue

    if not recent:
        slack_post(
            webhook,
            {
                "text": (
                    f":bar_chart: Weekly r/programmatic sentiment "
                    f"(last {LOOKBACK_DAYS}d): no new posts found."
                )
            },
        )
        return 0

    unigrams = Counter()
    bigrams = Counter()
    overall_pos = overall_neg = 0
    scored_posts = []
    total_comments = 0

    for p in recent:
        title = p.get("title") or ""
        body = p.get("content") or p.get("selftext") or ""
        text_parts = [title, body]
        for c in iter_comments(p.get("comments") or []):
            cb = c.get("body") or ""
            if cb:
                text_parts.append(cb)
                total_comments += 1
        full_text = "\n".join(text_parts)
        tokens = tokenize(full_text)

        topic_tokens = [t for t in tokens if is_topic_token(t)]
        unigrams.update(topic_tokens)
        for a, b in zip(topic_tokens, topic_tokens[1:]):
            bigrams[f"{a} {b}"] += 1

        score, pos, neg = sentiment_score(tokens)
        overall_pos += pos
        overall_neg += neg
        scored_posts.append(
            {
                "post": p,
                "score": score,
                "pos": pos,
                "neg": neg,
                "hits": pos + neg,
            }
        )

    overall_total = overall_pos + overall_neg
    overall_score = (
        (overall_pos - overall_neg) / overall_total if overall_total else 0.0
    )
    overall_label = label(overall_score, overall_total)

    top_topics = [t for t, _ in unigrams.most_common(10)]
    top_bigrams = [t for t, c in bigrams.most_common(8) if c >= 2]

    most_positive = sorted(
        [s for s in scored_posts if s["hits"] >= 2 and s["score"] > 0],
        key=lambda s: (-s["score"], -s["hits"]),
    )[:3]
    most_negative = sorted(
        [s for s in scored_posts if s["hits"] >= 2 and s["score"] < 0],
        key=lambda s: (s["score"], -s["hits"]),
    )[:3]

    def fmt_post_line(s):
        p = s["post"]
        title = p.get("title") or "(no title)"
        return (
            f"• <{post_url(p)}|{title}> "
            f"(score {s['score']:+.2f}, +{s['pos']}/-{s['neg']})"
        )

    sentiment_emoji = {
        "positive": ":green_circle:",
        "negative": ":red_circle:",
        "mixed": ":large_yellow_circle:",
        "neutral": ":white_circle:",
    }[overall_label]

    header = (
        f":bar_chart: *Weekly r/programmatic pulse — last {LOOKBACK_DAYS} days*\n"
        f"{sentiment_emoji} Overall mood: *{overall_label}* "
        f"(score {overall_score:+.2f}, {overall_pos} positive vs "
        f"{overall_neg} negative signals)\n"
        f"{len(recent)} posts · ~{total_comments} comments scanned"
    )

    topic_block = "*Popular topics:* " + (
        ", ".join(f"`{t}`" for t in top_topics) if top_topics else "_none_"
    )
    if top_bigrams:
        topic_block += "\n*Recurring phrases:* " + ", ".join(
            f"`{b}`" for b in top_bigrams
        )

    blocks = [
        {"type": "section", "text": {"type": "mrkdwn", "text": header}},
        {"type": "section", "text": {"type": "mrkdwn", "text": topic_block}},
    ]
    if most_positive:
        blocks.append(
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": ":green_circle: *Most positive threads*\n"
                    + "\n".join(fmt_post_line(s) for s in most_positive),
                },
            }
        )
    if most_negative:
        blocks.append(
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": ":red_circle: *Most negative threads*\n"
                    + "\n".join(fmt_post_line(s) for s in most_negative),
                },
            }
        )

    slack_post(
        webhook,
        {
            "text": (
                f"Weekly r/programmatic pulse: {overall_label} "
                f"({len(recent)} posts)"
            ),
            "blocks": blocks,
        },
    )
    print(
        f"Weekly digest sent. label={overall_label} score={overall_score:+.2f} "
        f"posts={len(recent)} comments={total_comments}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
