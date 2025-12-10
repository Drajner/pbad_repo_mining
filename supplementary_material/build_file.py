import requests
import time
import json
import gzip
import sys

OWNER = "kubernetes"
REPO = "kubernetes"

GITHUB_API_URL = f"https://api.github.com/repos/{OWNER}/{REPO}/events"
GITHUB_TOKEN = "changeme"

PER_PAGE = 100
MAX_PAGES = 10
OUTFILE_GZ = "k8s-events.json.gz"

def build_session():

    s = requests.Session()
    s.headers.update({
        "Accept": "application/vnd.github+json",
        "Authorization": f"Bearer {GITHUB_TOKEN}",
        "X-GitHub-Api-Version": "2022-11-28",
        "User-Agent": "gharchive-like-export/1.0",
    })
    return s

def fetch_events(session):
    for page in range(1, MAX_PAGES + 1):
        params = {"per_page": PER_PAGE, "page": page}
        resp = session.get(GITHUB_API_URL, params=params)

        if resp.status_code == 422:
            print(
                f"Received 422 status from GitHub API for page={page} – "
                "no more available events (~300)"
            )
            break

        if resp.status_code == 304:
            break

        if resp.status_code == 403 and "rate limit" in resp.text.lower():
            print("GitHub rate limit – headers:", resp.headers, file=sys.stderr)
            raise RuntimeError("GitHub API rate limit exceeded")

        resp.raise_for_status()
        events = resp.json()
        if not events:
            break

        for ev in events:
            yield ev

        time.sleep(0.2)


def event_to_gharchive_row(ev):
    return {
        "id": ev.get("id"),
        "type": ev.get("type"),
        "actor": ev.get("actor") or {},
        "repo": ev.get("repo") or {},
        "org": ev.get("org") or {},
        "payload": ev.get("payload") or {},
        "created_at": ev.get("created_at"),
    }


def main():
    session = build_session()
    total = 0

    with gzip.open(OUTFILE_GZ, "wt", encoding="utf-8") as f:
        for ev in fetch_events(session):
            row = event_to_gharchive_row(ev)
            f.write(json.dumps(row) + "\n")
            total += 1

    print(f"Saved {total} github events to {OUTFILE_GZ}")


if __name__ == "__main__":
    main()
