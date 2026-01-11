import gzip
import json
import time
from datetime import datetime, timezone
import requests
from typing import Callable, Dict, Any, Optional, Tuple, List

# =========================
# --- CONFIGURATION ---
# =========================
GITHUB_TOKEN = "changeme"
ORG_NAME = "Wiselibs"
OUTPUT_FILE = "wiselibs_newscript_to_2025.json.gz"
STOP_DATE = datetime(2025, 1, 1, tzinfo=timezone.utc) #pobieramy >= STOP_DATE
PER_PAGE = 100
REQUEST_TIMEOUT = 30

HEADERS = {
    "Authorization": f"token {GITHUB_TOKEN}",
    "Accept": "application/vnd.github.v3+json",
    "User-Agent": "org-activity-etl"
}

# =========================
# --- HELPERS ---
# =========================
def is_bot(user_login: str) -> bool:
    """Bot identification according to methodology """
    if not user_login:
        return False
    ll = user_login.lower()
    return ll.endswith("[bot]") or ll.endswith("bot")

def parse_dt(iso_str: str) -> datetime:
    # GitHub: "2025-01-01T00:00:00Z"
    return datetime.fromisoformat(iso_str.replace("Z", "+00:00")).astimezone(timezone.utc)

#parse date from github response to reliably compare timestamps (against STOP_DATE)
def dt_ge_stop(iso_str: str) -> bool:
    return parse_dt(iso_str) >= STOP_DATE

def handle_rate_limit(resp: requests.Response) -> bool:
    # primary rate limit
    if resp.status_code == 403 and resp.headers.get("X-RateLimit-Remaining") == "0":
        reset_time = resp.headers.get("X-RateLimit-Reset")
        if reset_time:
            reset_time = int(reset_time)
            sleep_duration = max(reset_time - int(time.time()), 0) + 5
            print(f"\n[LIMIT] Rate limit. Sleeping {datetime.fromtimestamp(reset_time, tz=timezone.utc).strftime('%H:%M:%S')} UTC ({sleep_duration}s)")
            time.sleep(sleep_duration)
            return True
    # 429 (secondary/abuse/throttling)
    if resp.status_code == 429:
        ra = resp.headers.get("Retry-After")
        sleep_duration = int(ra) if (ra and ra.isdigit()) else 60
        print(f"\n[LIMIT] 429 Retry-After={ra}. Sleeping {sleep_duration}s...")
        time.sleep(sleep_duration)
        return True
    return False

def make_request(url: str, params: Optional[dict] = None) -> Optional[requests.Response]:
    backoff = 2
    for attempt in range(6):
        try:
            resp = requests.get(url, headers=HEADERS, params=params, timeout=REQUEST_TIMEOUT)

            if handle_rate_limit(resp):
                continue

            if resp.status_code >= 400:
                try:
                    j = resp.json()
                except Exception:
                    j = {"raw": resp.text[:300]}
                print(f"\n[HTTP {resp.status_code}] {url} params={params} msg={j.get('message')}")

            resp.raise_for_status()
            return resp

        except Exception as e:
            print(f"  [BŁĄD] {e}. Próba {attempt+1}/6... (sleep {backoff}s)")
            time.sleep(backoff)
            backoff = min(backoff * 2, 60)

    return None

# The prefix is added to the event ID to guarantee global uniqueness
# across different event types (issues, comments, pull requests),
# since object IDs from the GitHub API are only unique within their own type,
# unlike GHArchive’s native event IDs.
def event_id(prefix: str, obj_id: int | str) -> str:
    return f"{prefix}:{obj_id}"

#pagination abstraction
def list_all(url: str, base_params: dict) -> list:
    out = []
    page = 1
    while True:
        params = dict(base_params)
        params.update({"per_page": PER_PAGE, "page": page})
        resp = make_request(url, params)
        if not resp:
            break
        items = resp.json()
        if not items:
            break
        out.extend(items)
        if len(items) < PER_PAGE:
            break
        page += 1
    return out

def save_event(
    f_out,
    event_type: str,
    created_at_iso: str,
    actor_dict: dict,
    repo_dict: dict,
    org_dict: dict,
    payload_dict: dict,
    eid: str,
):
    """
    Zachowujemy format RAW GHArchive-like:
    {id,type,actor,repo,org,payload,created_at}
    gdzie actor/repo/org/payload są Stringami zawierającymi JSON.
    """
    event = {
        "id": eid,
        "type": event_type,
        "actor": json.dumps(actor_dict or {}, ensure_ascii=False),
        "repo": json.dumps(repo_dict or {}, ensure_ascii=False),
        "org": json.dumps(org_dict or {}, ensure_ascii=False),
        "payload": json.dumps(payload_dict or {}, ensure_ascii=False),
        "created_at": created_at_iso,
    }
    json.dump(event, f_out, ensure_ascii=False)
    f_out.write("\n")

def iterate_items_until_stop(
    url: str,
    params_base: dict,
    get_created_at: Callable[[dict], Optional[str]],
    on_item: Callable[[dict, str], None],
) -> int:
    """
    Standard loop for paginated endpoints sorted by created desc.
    Stops when created_at < STOP_DATE (early exit).
    """
    page = 1
    count = 0

    while True:
        params = dict(params_base)
        params.update({"per_page": PER_PAGE, "page": page})

        resp = make_request(url, params)
        if not resp:
            break
        items = resp.json()
        if not items:
            break

        stop_now = False
        for item in items:
            created_at = get_created_at(item)
            if not created_at:
                continue
            if not dt_ge_stop(created_at):
                stop_now = True
                continue

            on_item(item, created_at)
            count += 1

        if stop_now or len(items) < PER_PAGE:
            break
        page += 1

    return count


# =========================
# GET DATA FROM GITHUB API
# =========================
def get_org_dict(org: str) -> dict:
    org_resp = make_request(f"https://api.github.com/orgs/{org}")
    org_id = int(org_resp.json().get("id") or 0) if org_resp else 0
    return {"id": org_id, "login": org, "url": f"https://api.github.com/orgs/{org}"}

# GET /orgs/{org}/repos
def get_all_org_repos(org: str) -> List[str]:
    repos = []
    page = 1
    while True:
        url = f"https://api.github.com/orgs/{org}/repos"
        resp = make_request(url, {"per_page": PER_PAGE, "page": page, "type": "all", "sort": "full_name", "direction": "asc"})
        if not resp:
            break
        items = resp.json()
        if not items:
            break
        repos.extend([r["name"] for r in items])
        if len(items) < PER_PAGE:
            break
        page += 1
    return repos

#GET /repos/{owner}/{repo} - pobiera metadane repozytorium
def get_repo_meta(owner: str, repo: str) -> dict:
    url = f"https://api.github.com/repos/{owner}/{repo}"
    resp = make_request(url)
    if not resp:
        return {}
    r = resp.json()
    lic = r.get("license") or {}
    return {
        "id": int(r.get("id") or 0),
        "full_name": r.get("full_name") or f"{owner}/{repo}",
        "description": r.get("description") or "",
        "language": r.get("language") or "",
        "stargazers_count": int(r.get("stargazers_count") or 0),
        "forks_count": int(r.get("forks_count") or 0),
        "size": int(r.get("size") or 0),
        "has_issues": bool(r.get("has_issues")),
        "has_projects": bool(r.get("has_projects")),
        "has_downloads": bool(r.get("has_downloads")),
        "has_wiki": bool(r.get("has_wiki")),
        "has_pages": bool(r.get("has_pages")),
        "default_branch": r.get("default_branch") or "",
        "license": lic.get("spdx_id") or lic.get("key") or "",
    }

def build_repo_context(owner: str, repo: str) -> Tuple[str, dict, dict]:
    """
    Returns:
      base_url, repo_dict, repository_payload (for payload.repository.*)
    """
    base_url = f"https://api.github.com/repos/{owner}/{repo}"
    meta = get_repo_meta(owner, repo)

    repo_dict = {
        "id": int(meta.get("id") or 0),
        "name": meta.get("full_name") or f"{owner}/{repo}",
        "url": f"https://api.github.com/repos/{owner}/{repo}",
    }

    repository_payload = {
        "id": repo_dict["id"],
        "name": repo_dict["name"],
        "description": meta.get("description") or "",
        "language": meta.get("language") or "",
        "stargazers_count": int(meta.get("stargazers_count") or 0),
        "forks_count": int(meta.get("forks_count") or 0),
        "size": int(meta.get("size") or 0),
        "has_issues": bool(meta.get("has_issues")),
        "has_projects": bool(meta.get("has_projects")),
        "has_downloads": bool(meta.get("has_downloads")),
        "has_wiki": bool(meta.get("has_wiki")),
        "has_pages": bool(meta.get("has_pages")),
        "default_branch": meta.get("default_branch") or "",
        "license": meta.get("license") or "",
    }

    return base_url, repo_dict, repository_payload

# =========================
# EMITTERS (per event type)
# =========================

def emit_issue_comment_events(f_out, base_url: str, repo_dict: dict, org_dict: dict, repository_payload: dict) -> int:
    url = f"{base_url}/issues/comments"

    def on_item(c: dict, created_at: str) -> None:
        actor = c.get("user") or {}
        payload = {"action": "created", "comment": c, "repository": repository_payload}
        save_event(
            f_out,
            "IssueCommentEvent",
            created_at,
            actor,
            repo_dict,
            org_dict,
            payload,
            event_id("issue_comment", c.get("id")),
        )

    return iterate_items_until_stop(
        url=url,
        params_base={"sort": "created", "direction": "desc"},
        get_created_at=lambda c: c.get("created_at"),
        on_item=on_item,
    )


def emit_issue_opened_events(f_out, base_url: str, repo_dict: dict, org_dict: dict, repository_payload: dict) -> int:
    url = f"{base_url}/issues"

    def on_item(it: dict, created_at: str) -> None:
        if "pull_request" in it:
            return
        actor = it.get("user") or {}
        payload = {"action": "opened", "issue": it, "repository": repository_payload}
        save_event(
            f_out,
            "IssuesEvent",
            created_at,
            actor,
            repo_dict,
            org_dict,
            payload,
            event_id("issue_open", it.get("id") or it.get("number")),
        )

    return iterate_items_until_stop(
        url=url,
        params_base={"state": "all", "sort": "created", "direction": "desc"},
        get_created_at=lambda it: it.get("created_at"),
        on_item=on_item,
    )


def fetch_pr_full(base_url: str, pr_number: int) -> dict:
    resp = make_request(f"{base_url}/pulls/{pr_number}")
    return resp.json() if resp else {}


def normalize_requested_reviewer(pr_full: dict) -> None:
    # loader CH expects payload.pull_request.requested_reviewer.* (single object),
    # GitHub gives requested_reviewers (list). We map the first one.
    rr = pr_full.get("requested_reviewers") or []
    if rr:
        pr_full["requested_reviewer"] = rr[0]


def emit_pull_request_events(f_out, base_url: str, repo_dict: dict, org_dict: dict, repository_payload: dict) -> int:
    url = f"{base_url}/pulls"

    emitted = 0

    def on_item(pr: dict, created_at: str) -> None:
        nonlocal emitted
        pr_number = int(pr.get("number") or 0)
        if pr_number <= 0:
            return

        pr_full = fetch_pr_full(base_url, pr_number) or pr
        normalize_requested_reviewer(pr_full)

        actor_author = pr_full.get("user") or {}

        # opened
        payload_open = {"action": "opened", "pull_request": pr_full, "repository": repository_payload}
        save_event(
            f_out,
            "PullRequestEvent",
            created_at,
            actor_author,
            repo_dict,
            org_dict,
            payload_open,
            event_id("pr_open", pr_full.get("id") or pr_number),
        )
        emitted += 1

        # merged -> emit as closed event, but merged=true is inside pull_request
        merged_at = pr_full.get("merged_at")
        if merged_at and dt_ge_stop(merged_at):
            merger = pr_full.get("merged_by") or actor_author
            payload_merged = {"action": "closed", "pull_request": pr_full, "repository": repository_payload}
            save_event(
                f_out,
                "PullRequestEvent",
                merged_at,
                merger,
                repo_dict,
                org_dict,
                payload_merged,
                event_id("pr_merged", pr_full.get("id") or pr_number),
            )
            emitted += 1

    # paginacja “po created_at”; stop warunek = created_at < STOP_DATE
    iterate_items_until_stop(
        url=url,
        params_base={"state": "all", "sort": "created", "direction": "desc"},
        get_created_at=lambda pr: pr.get("created_at"),
        on_item=on_item,
    )

    return emitted


def emit_pr_review_comment_events(f_out, base_url: str, repo_dict: dict, org_dict: dict, repository_payload: dict) -> int:
    url = f"{base_url}/pulls/comments"

    def on_item(c: dict, created_at: str) -> None:
        actor = c.get("user") or {}
        payload = {"action": "created", "comment": c, "repository": repository_payload}
        save_event(
            f_out,
            "PullRequestReviewCommentEvent",
            created_at,
            actor,
            repo_dict,
            org_dict,
            payload,
            event_id("pr_review_comment", c.get("id")),
        )

    return iterate_items_until_stop(
        url=url,
        params_base={"sort": "created", "direction": "desc"},
        get_created_at=lambda c: c.get("created_at"),
        on_item=on_item,
    )

# =========================
# ORCHESTRATION
# =========================
def process_repo(f_out, owner: str, repo: str, org_dict: dict) -> None:
    base_url, repo_dict, repository_payload = build_repo_context(owner, repo)

    print("  repo metadata ok")

    c1 = emit_issue_comment_events(f_out, base_url, repo_dict, org_dict, repository_payload)
    print(f"  issue comments: {c1}")

    c2 = emit_issue_opened_events(f_out, base_url, repo_dict, org_dict, repository_payload)
    print(f"  issues opened: {c2}")

    c3 = emit_pull_request_events(f_out, base_url, repo_dict, org_dict, repository_payload)
    print(f"  pull requests (opened+merged): {c3}")

    c4 = emit_pr_review_comment_events(f_out, base_url, repo_dict, org_dict, repository_payload)
    print(f"  pr review comments: {c4}")


def main():
    if not GITHUB_TOKEN:
        print("Ustaw GITHUB_TOKEN.")
        return

    org_dict = get_org_dict(ORG_NAME)
    repos = get_all_org_repos(ORG_NAME)

    print(f"Start: org={ORG_NAME}, repos={len(repos)}, STOP_DATE>={STOP_DATE.isoformat()}")

    with gzip.open(OUTPUT_FILE, "wt", encoding="utf-8") as f_out:
        for idx, repo in enumerate(repos, 1):
            print(f"\n[{idx}/{len(repos)}] {ORG_NAME}/{repo}")
            process_repo(f_out, ORG_NAME, repo, org_dict)

    print(f"\nDone. Output: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()