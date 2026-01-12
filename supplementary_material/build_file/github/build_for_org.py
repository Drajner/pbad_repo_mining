import gzip
import json
import time
from datetime import datetime, timezone
import requests
from typing import Callable, Optional, Tuple, List
from zoneinfo import ZoneInfo
import re

GITHUB_TOKEN = "chengeme"
ORG_NAME = "grafana"
OUTPUT_FILE = "output_files/grafana_performance_100repos_test_to_2025.json.gz"
STOP_DATE = datetime(2025, 1, 1, tzinfo=timezone.utc)

PER_PAGE = 100
REQUEST_TIMEOUT = 30
REPO_NUMBER_LIMIT = 100

HEADERS = {
    "Authorization": f"token {GITHUB_TOKEN}",
    "Accept": "application/vnd.github.v3+json",
    "User-Agent": "org-activity-etl",
}

SESSION = requests.Session()
SESSION.headers.update(HEADERS)

WARSAW_TZ = ZoneInfo("Europe/Warsaw")
_LINK_NEXT_RE = re.compile(r'<([^>]+)>;\s*rel="next"')

def _fmt_ts(ts: int) -> tuple[str, str]:
    dt_utc = datetime.fromtimestamp(ts, tz=timezone.utc)
    dt_pl = dt_utc.astimezone(WARSAW_TZ)
    return dt_utc.strftime("%H:%M:%S UTC"), dt_pl.strftime("%H:%M:%S %Z")


def parse_dt(iso_str: str) -> datetime:
    return datetime.fromisoformat(iso_str.replace("Z", "+00:00")).astimezone(timezone.utc)


def dt_ge_stop(iso_str: str) -> bool:
    return parse_dt(iso_str) >= STOP_DATE


def get_next_link(resp: requests.Response) -> Optional[str]:
    link = resp.headers.get("Link") or ""
    m = _LINK_NEXT_RE.search(link)
    return m.group(1) if m else None


def handle_rate_limit(resp: requests.Response) -> bool:
    # primary rate limit
    if resp.status_code == 403 and resp.headers.get("X-RateLimit-Remaining") == "0":
        reset = resp.headers.get("X-RateLimit-Reset")
        if reset and reset.isdigit():
            reset_ts = int(reset)
            sleep_s = max(reset_ts - int(time.time()), 0) + 5
            utc_s, pl_s = _fmt_ts(reset_ts)
            print(
                f"\n[LIMIT] Primary rate limit. Wznowienie {pl_s} / {utc_s} (sleep {sleep_s}s)"
            )
            time.sleep(sleep_s)
            return True

        sleep_s = 60
        resume_ts = int(time.time()) + sleep_s
        utc_s, pl_s = _fmt_ts(resume_ts)
        print(f"\n[LIMIT] Primary rate limit. Wznowienie {pl_s} / {utc_s} (sleep {sleep_s}s)")
        time.sleep(sleep_s)
        return True

    # secondary rate limit (abuse detection)
    if resp.status_code == 403:
        ra = resp.headers.get("Retry-After")
        if ra and ra.isdigit():
            sleep_s = int(ra) + 2
            resume_ts = int(time.time()) + sleep_s
            utc_s, pl_s = _fmt_ts(resume_ts)
            print(
                f"\n[LIMIT] Secondary rate limit. Wznowienie {pl_s} / {utc_s} (sleep {sleep_s}s)"
            )
            time.sleep(sleep_s)
            return True

    # too many requests
    if resp.status_code == 429:
        ra = resp.headers.get("Retry-After")
        sleep_s = int(ra) if (ra and ra.isdigit()) else 60
        sleep_s += 2
        resume_ts = int(time.time()) + sleep_s
        utc_s, pl_s = _fmt_ts(resume_ts)
        print(f"\n[LIMIT] 429 Too Many Requests. Wznowienie {pl_s} / {utc_s} (sleep {sleep_s}s)")
        time.sleep(sleep_s)
        return True

    return False


def make_request(url: str, params: Optional[dict] = None) -> Optional[requests.Response]:
    backoff = 2
    for attempt in range(6):
        try:
            resp = SESSION.get(url, params=params, timeout=REQUEST_TIMEOUT)

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


def iterate_items_until_stop_link(
    url: str,
    params_base: dict,
    get_created_at: Callable[[dict], Optional[str]],
    on_item: Callable[[dict, str], None],
) -> int:
    count = 0

    next_url: Optional[str] = url
    first_params = dict(params_base)
    first_params["per_page"] = PER_PAGE

    while next_url:
        resp = make_request(next_url, first_params if next_url == url else None)
        if not resp:
            break

        items = resp.json() or []
        if not items:
            break

        for item in items:
            created_at = get_created_at(item)
            if not created_at:
                continue
            if not dt_ge_stop(created_at):
                return count
            on_item(item, created_at)
            count += 1

        next_url = get_next_link(resp)
        first_params = None

    return count

def event_id(prefix: str, obj_id: int | str) -> str:
    return f"{prefix}:{obj_id}"

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


def get_org_metadata(org: str) -> dict:
    org_resp = make_request(f"https://api.github.com/orgs/{org}")
    org_id = int(org_resp.json().get("id") or 0) if org_resp else 0
    return {"id": org_id, "login": org, "url": f"https://api.github.com/orgs/{org}"}


def get_org_repos(org: str, limit: int = REPO_NUMBER_LIMIT) -> List[str]:
    resp = make_request(
        f"https://api.github.com/orgs/{org}/repos",
        {"per_page": min(limit, 100), "page": 1},
    )
    return [r["name"] for r in resp.json()] if resp else []


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


# IssueCommentEvent: action=created (comments in issues/PRs)
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

    return iterate_items_until_stop_link(
        url=url,
        params_base={"sort": "created", "direction": "desc"},
        get_created_at=lambda c: c.get("created_at"),
        on_item=on_item,
    )


# IssuesEvent: action=opened
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

    return iterate_items_until_stop_link(
        url=url,
        params_base={"state": "all", "sort": "created", "direction": "desc"},
        get_created_at=lambda it: it.get("created_at"),
        on_item=on_item,
    )


# PullRequestEvent: action=opened + PullRequestEvent: action=closed with pull_merged=1
def emit_pull_request_events(
    f_out,
    base_url: str,
    repo_dict: dict,
    org_dict: dict,
    repository_payload: dict,
) -> int:
    emitted = 0

    def save_opened(pr: dict, created_at: str) -> None:
        nonlocal emitted
        actor_author = pr.get("user") or {}
        payload_open = {"action": "opened", "pull_request": pr, "repository": repository_payload}
        save_event(
            f_out,
            "PullRequestEvent",
            created_at,
            actor_author,
            repo_dict,
            org_dict,
            payload_open,
            event_id("pr_open", pr.get("id") or pr.get("number")),
        )
        emitted += 1

    def save_merged(pr_obj: dict, merged_at: str) -> None:
        nonlocal emitted
        pr_number = int(pr_obj.get("number") or 0)

        # kto wykonał merge
        merger = (pr_obj.get("merged_by") or pr_obj.get("user") or {})
        payload_merged = {
            "action": "closed",
            "pull_merged": 1,
            "pull_request": pr_obj,
            "repository": repository_payload,
        }
        save_event(
            f_out,
            "PullRequestEvent",
            merged_at,
            merger,
            repo_dict,
            org_dict,
            payload_merged,
            event_id("pr_merged", pr_obj.get("id") or pr_number),
        )
        emitted += 1

    # 1) PR opened
    emitted += iterate_items_until_stop_link(
        url=f"{base_url}/pulls",
        params_base={"state": "all", "sort": "created", "direction": "desc"},
        get_created_at=lambda pr: pr.get("created_at"),
        on_item=lambda pr, created_at: save_opened(pr, created_at),
    )

    # 2) PR merged
    def on_closed_item(pr: dict, updated_at: str) -> None:
        merged_at = pr.get("merged_at")
        if merged_at and dt_ge_stop(merged_at):
            save_merged(pr, merged_at)

    emitted += iterate_items_until_stop_link(
        url=f"{base_url}/pulls",
        params_base={"state": "closed", "sort": "updated", "direction": "desc"},
        get_created_at=lambda pr: pr.get("updated_at"),
        on_item=on_closed_item,
    )

    return emitted


# PullRequestReviewCommentEvent: action=created
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

    return iterate_items_until_stop_link(
        url=url,
        params_base={"sort": "created", "direction": "desc"},
        get_created_at=lambda c: c.get("created_at"),
        on_item=on_item,
    )


def run_fetch(category_name: str, fn) -> None:
    print(f"  -> {category_name}...", end="", flush=True)
    count = fn()
    print(f" [Suma: {count}]")


def process_repo(f_out, org: str, repo: str, org_dict: dict) -> None:
    print("Pobieranie metadanych..", end="", flush=True)
    base_url, repo_dict, repository_payload = build_repo_context(org, repo)
    print(" OK")

    run_fetch(
        "Komentarze (IssueCommentEvent)",
        lambda: emit_issue_comment_events(f_out, base_url, repo_dict, org_dict, repository_payload),
    )
    run_fetch(
        "Issues opened (IssuesEvent)",
        lambda: emit_issue_opened_events(f_out, base_url, repo_dict, org_dict, repository_payload),
    )
    run_fetch(
        "Pull Requests (opened + merged)",
        lambda: emit_pull_request_events(f_out, base_url, repo_dict, org_dict, repository_payload),
    )
    run_fetch(
        "Review comments (PullRequestReviewCommentEvent)",
        lambda: emit_pr_review_comment_events(f_out, base_url, repo_dict, org_dict, repository_payload),
    )


def main():
    if not GITHUB_TOKEN:
        print("Ustaw GITHUB_TOKEN.")
        return

    org_metadata = get_org_metadata(ORG_NAME)
    repos = get_org_repos(ORG_NAME, limit=100)
    if not repos:
        print("Brak repozytoriów w danej organizacji")
        return

    print(f"Start: org={ORG_NAME}, repo={len(repos)}, STOP_DATE>={STOP_DATE.isoformat()}")

    with gzip.open(OUTPUT_FILE, "wt", encoding="utf-8") as f_out:
        for idx, repo in enumerate(repos, 1):
            print(f"\n[{idx}/{len(repos)}] {ORG_NAME}/{repo}")
            process_repo(f_out, ORG_NAME, repo, org_metadata)

    print(f"\nZakończone. Plik: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
