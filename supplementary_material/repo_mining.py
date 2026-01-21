import gzip
import hashlib
import json
import os
import time
from datetime import datetime, timezone
import requests
from typing import Callable, Optional, Tuple, List, Any
from zoneinfo import ZoneInfo
import re

# DO ZMIANY ==================
GITHUB_TOKEN = os.environ["GITHUB_TOKEN"]
EXCLUDED_REPOS = [
    "grafana/grafana",
    "grafana/k6",
    "grafana/loki",
    "grafana/grafana-image-renderer",
]
ORG_NAME = "grafana"
REPO_NUMBER_LIMIT = 20
# ==================

OUTPUT_FILE = "repo_mining_wynik.json.gz"
STOP_DATE = datetime(2025, 1, 14, tzinfo=timezone.utc)

PER_PAGE = 100
REQUEST_TIMEOUT = 30

HEADERS = {
    "Authorization": f"token {GITHUB_TOKEN}",
    "Accept": "application/vnd.github.v3+json",
    "User-Agent": "org-activity-etl",
}

SESSION = requests.Session()
SESSION.headers.update(HEADERS)

WARSAW_TZ = ZoneInfo("Europe/Warsaw")
_LINK_NEXT_RE = re.compile(r'<([^>]+)>;\s*rel="next"')

ANONYMIZATION_SALT = "D0NotShareThisSalt123!"
REDACTED_BODY = "xyz"

def is_bot(user_data: dict) -> bool:
    if not user_data:
        return False

    if user_data.get('type') == 'Bot':
        return True

    login = user_data.get('login', '')
    if login and str(login).lower().endswith('[bot]'):
        return True

    return False

def get_hash_str(value: Any) -> Optional[str]:
    if value is None:
        return None
    raw = f"{ANONYMIZATION_SALT}:{value}"
    return hashlib.sha256(raw.encode('utf-8')).hexdigest()

def get_hash_int(value: Any) -> Optional[int]:
    if value is None:
        return None
    raw = f"{ANONYMIZATION_SALT}:{value}"
    hex_hash = hashlib.sha256(raw.encode('utf-8')).hexdigest()
    return int(hex_hash[:13], 16)

def process_user_data(user_data: dict) -> Optional[dict]:
    if not user_data:
        return None

    if is_bot(user_data):
        return {
            'id': user_data.get('id'),
            'login': user_data.get('login'),
            'type': 'Bot'
        }

    return {
        'id': get_hash_int(user_data.get('id')),
        'login': get_hash_str(user_data.get('login')),
        'type': user_data.get('type')
    }

def filter_event_for_clickhouse(raw_event):
    if not raw_event:
        return None

    filtered = {
        'id': raw_event.get('id'),
        'type': raw_event.get('type'),
        'created_at': raw_event.get('created_at'),
    }

    if 'actor' in raw_event:
        filtered['actor'] = process_user_data(raw_event['actor'])

    if 'repo' in raw_event:
        filtered['repo'] = {
            'id': get_hash_int(raw_event['repo'].get('id')),
            'name': get_hash_str(raw_event['repo'].get('name'))
        }

    if 'org' in raw_event:
        filtered['org'] = {
            'id': get_hash_int(raw_event['org'].get('id')),
            'login': get_hash_str(raw_event['org'].get('login'))
        }

    if 'payload' in raw_event:
        p = raw_event['payload']
        new_p = {}

        if 'action' in p: new_p['action'] = p['action']
        if 'push_id' in p: new_p['push_id'] = p['push_id']
        if 'size' in p: new_p['size'] = p['size']
        if 'distinct_size' in p: new_p['distinct_size'] = p['distinct_size']
        if 'ref' in p: new_p['ref'] = p['ref']
        if 'head' in p: new_p['head'] = p['head']
        if 'before' in p: new_p['before'] = p['before']
        if 'ref_type' in p: new_p['ref_type'] = p['ref_type']
        if 'master_branch' in p: new_p['master_branch'] = p['master_branch']
        if 'description' in p: new_p['description'] = REDACTED_BODY
        if 'pusher_type' in p: new_p['pusher_type'] = p['pusher_type']

        if 'repository' in p and p['repository']:
            repo_src = p['repository']
            new_p['repository'] = {
                'description': REDACTED_BODY,
                'language': repo_src.get('language'),
                'stargazers_count': repo_src.get('stargazers_count'),
                'forks_count': repo_src.get('forks_count'),
                'size': repo_src.get('size'),
                'has_issues': repo_src.get('has_issues'),
                'has_projects': repo_src.get('has_projects'),
                'has_downloads': repo_src.get('has_downloads'),
                'has_wiki': repo_src.get('has_wiki'),
                'has_pages': repo_src.get('has_pages'),
                'default_branch': repo_src.get('default_branch'),
            }

        if 'issue' in p and p['issue']:
            iss = p['issue']
            new_iss = {
                'id': iss.get('id'),
                'number': iss.get('number'),
                'title': REDACTED_BODY,
                'body': REDACTED_BODY,
                'comments': iss.get('comments'),
                'author_association': iss.get('author_association'),
            }
            if 'user' in iss:
                new_iss['user'] = process_user_data(iss['user'])
            if 'assignee' in iss:
                new_iss['assignee'] = process_user_data(iss['assignee'])
            new_p['issue'] = new_iss

        if 'comment' in p and p['comment']:
            com = p['comment']
            new_com = {
                'id': com.get('id'),
                'body': REDACTED_BODY,
                'author_association': com.get('author_association'),
                'path': get_hash_str(com.get('path')),
                'position': com.get('position'),
                'line': com.get('line'),
            }
            if 'user' in com:
                new_com['user'] = process_user_data(com['user'])
            new_p['comment'] = new_com

        if 'pull_request' in p and p['pull_request']:
            pr = p['pull_request']
            new_pr = {
                'merged': pr.get('merged'),
                'merge_commit_sha': get_hash_str(pr.get('merge_commit_sha')),
                'commits': pr.get('commits'),
                'additions': pr.get('additions'),
                'deletions': pr.get('deletions'),
                'changed_files': pr.get('changed_files'),
                'review_comments': pr.get('review_comments'),
            }
            if 'merged_by' in pr:
                new_pr['merged_by'] = process_user_data(pr['merged_by'])
            if 'requested_reviewer' in pr:
                new_pr['requested_reviewer'] = process_user_data(pr['requested_reviewer'])
            new_p['pull_request'] = new_pr

        if 'forkee' in p and p['forkee']:
            fork = p['forkee']
            new_fork = {
                'id': fork.get('id'),
                'full_name': fork.get('full_name'),
            }
            if 'owner' in fork:
                new_fork['owner'] = process_user_data(fork['owner'])
            new_p['forkee'] = new_fork

        if 'member' in p:
            new_p['member'] = process_user_data(p['member'])

        if 'release' in p and p['release']:
            rel = p['release']
            new_rel = {
                'id': rel.get('id'),
                'tag_name': rel.get('tag_name'),
                'target_commitish': rel.get('target_commitish'),
                'name': rel.get('name'),
                'draft': rel.get('draft'),
                'prerelease': rel.get('prerelease'),
                'body': REDACTED_BODY,
            }
            if 'author' in rel:
                new_rel['author'] = process_user_data(rel['author'])
            new_p['release'] = new_rel

        filtered['payload'] = new_p

    return filtered

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
    for attempt in range(3):
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
    raw_event = {
        "id": eid,
        "type": event_type,
        "created_at": created_at_iso,
        "actor": actor_dict,
        "repo": repo_dict,
        "org": org_dict,
        "payload": payload_dict,
    }
    optimized_event = filter_event_for_clickhouse(raw_event)
    if optimized_event:
        f_out.write(json.dumps(optimized_event, ensure_ascii=False) + "\n")


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
    }

    return base_url, repo_dict, repository_payload


# IssueCommentEvent: action=created
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


# PullRequestEvent
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

        # 1) PR opened
    emitted += iterate_items_until_stop_link(
        url=f"{base_url}/pulls",
        params_base={"state": "all", "sort": "created", "direction": "desc"},
        get_created_at=lambda pr: pr.get("created_at"),
        on_item=lambda pr, created_at: save_opened(pr, created_at),
    )

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
    if not ORG_NAME:
        print("[ERROR] Nie podano nazwy organizacji (ORG_NAME).")
        return

    print(f"[START] Rozpoczecie pobierania danych dla org={ORG_NAME}, limit_repo={REPO_NUMBER_LIMIT}, DATE>={STOP_DATE.isoformat()}")
    org_metadata = get_org_metadata(ORG_NAME)
    url = f"https://api.github.com/orgs/{ORG_NAME}/repos"
    params = {"per_page": 100, "type": "all"}
    with gzip.open(OUTPUT_FILE, "wt", encoding="utf-8") as f_out:
        processed_count = 0

        while url:
            if processed_count >= REPO_NUMBER_LIMIT:
                print(f"[LIMIT] Osiągnięto limit {REPO_NUMBER_LIMIT} repozytoriów")
                break

            resp = make_request(url, params)
            if not resp:
                break

            repos_page = resp.json() or []
            if not repos_page:
                break

            for repo_data in repos_page:
                if processed_count >= REPO_NUMBER_LIMIT:
                    break

                full_name = repo_data.get("full_name", "")
                name = repo_data.get("name", "")

                if full_name in EXCLUDED_REPOS:
                    print(f"[SKIP] Pominięto repozytorium: {full_name} (na liście EXCLUDED_REPOS)")
                    continue

                processed_count += 1
                print(f"\n[{processed_count}] {full_name}")
                process_repo(f_out, ORG_NAME, name, org_metadata)

            if processed_count >= REPO_NUMBER_LIMIT:
                print(f"\n[LIMIT] Osiągnięto limit {REPO_NUMBER_LIMIT} repozytoriów.")
                break

            url = get_next_link(resp)
            params = None

    print(f"\nWynikowy plik: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
