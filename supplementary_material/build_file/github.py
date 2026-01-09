# badane typy aktywnosci:
# IssueCommentEvent
# IssuesEvent
# PullRequestEvent (opened)
# PullRequestReviewCommentEvent
# PullRequestEvent (closed & pull_merged=1)
import os
import re
import json
import gzip
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import requests

# =========================
# CONFIG
# =========================
ORG_NAME = "kubernetes"
OUTPUT_FILE = "org_activity_events.json.gz"

# Fetch data for items created >= STOP_DATE (inclusive)
STOP_DATE = datetime(2024, 1, 1, tzinfo=timezone.utc)

GITHUB_TOKEN = os.getenv("GITHUB_TOKEN", "token")

HEADERS = {
    "Authorization": f"token {GITHUB_TOKEN}",
    "Accept": "application/vnd.github.v3+json",
    "User-Agent": "org-activity-etl",
}

PER_PAGE = 100
REQUEST_TIMEOUT = 30

# =========================
# HELPERS
# =========================

def iso_to_ch_datetime(iso_str: Optional[str]) -> str:
    """ClickHouse-friendly DateTime string: 'YYYY-MM-DD HH:MM:SS' or epoch default."""
    if not iso_str:
        return "1970-01-01 00:00:00"
    dt = datetime.fromisoformat(iso_str.replace("Z", "+00:00"))
    dt = dt.astimezone(timezone.utc)
    return dt.strftime("%Y-%m-%d %H:%M:%S")

def iso_to_ch_date(iso_str: Optional[str]) -> str:
    if not iso_str:
        return "1970-01-01"
    dt = datetime.fromisoformat(iso_str.replace("Z", "+00:00"))
    dt = dt.astimezone(timezone.utc)
    return dt.strftime("%Y-%m-%d")

def dt_ge_stop(iso_str: str) -> bool:
    dt = datetime.fromisoformat(iso_str.replace("Z", "+00:00")).astimezone(timezone.utc)
    return dt >= STOP_DATE

def request_json(url: str, params: Optional[dict] = None) -> Tuple[int, Any, Dict[str, str]]:
    """GET JSON with basic rate-limit handling & retries."""
    backoff = 1.0
    for attempt in range(8):
        resp = requests.get(url, headers=HEADERS, params=params, timeout=REQUEST_TIMEOUT)
        status = resp.status_code

        # Rate limit handling
        if status == 403 and resp.headers.get("X-RateLimit-Remaining") == "0":
            reset = resp.headers.get("X-RateLimit-Reset")
            if reset:
                sleep_for = max(0, int(reset) - int(time.time()) + 2)
                print(f"Rate limit hit. Sleeping {sleep_for}s...")
                time.sleep(sleep_for)
                continue

        # Retry transient errors
        if status in (502, 503, 504, 522, 524):
            print(f"Transient error {status} on {url}. Backoff {backoff}s (attempt {attempt+1})")
            time.sleep(backoff)
            backoff = min(backoff * 2, 30)
            continue

        try:
            data = resp.json() if resp.content else None
        except Exception:
            data = None

        return status, data, dict(resp.headers)

    return 599, None, {}

def paginate(url: str, params: dict) -> List[Any]:
    """Simple page-based pagination (page=1..n)."""
    out = []
    page = 1
    while True:
        p = dict(params)
        p["per_page"] = PER_PAGE
        p["page"] = page
        status, data, _ = request_json(url, p)
        if status != 200 or not data:
            break
        out.extend(data)
        if len(data) < PER_PAGE:
            break
        page += 1
    return out

def parse_issue_number_from_url(issue_url: str) -> Optional[int]:
    # issue_url typically: https://api.github.com/repos/{owner}/{repo}/issues/{number}
    m = re.search(r"/issues/(\d+)$", issue_url)
    return int(m.group(1)) if m else None

def parse_pr_number_from_url(pr_url: str) -> Optional[int]:
    # pr_url typically: https://api.github.com/repos/{owner}/{repo}/pulls/{number}
    m = re.search(r"/pulls/(\d+)$", pr_url)
    return int(m.group(1)) if m else None

def default_row() -> Dict[str, Any]:
    """Default row matching your ClickHouse table columns (non-nullable safe defaults)."""
    # NOTE: We only populate the columns relevant to the events you asked for.
    # Everything else stays at CH-friendly defaults.
    return {
        "id": "",
        "type": "",
        "action": "",
        "actor_id": 0,
        "actor_login": "",
        "repo_id": 0,
        "repo_name": "",
        "org_id": 0,
        "org_login": "",
        "created_at": "1970-01-01 00:00:00",
        "created_date": "1970-01-01",

        "issue_id": 0,
        "issue_number": 0,
        "issue_title": "",
        "issue_body": "",
        "issue_author_id": 0,
        "issue_author_login": "",
        "issue_author_type": "",
        "issue_author_association": "",
        "issue_assignee_id": 0,
        "issue_assignee_login": "",
        "issue_created_at": "1970-01-01 00:00:00",
        "issue_updated_at": "1970-01-01 00:00:00",
        "issue_comments": 0,
        "issue_closed_at": "1970-01-01 00:00:00",

        "issue_comment_id": 0,
        "issue_comment_body": "",
        "issue_comment_created_at": "1970-01-01 00:00:00",
        "issue_comment_updated_at": "1970-01-01 00:00:00",
        "issue_comment_author_association": "",
        "issue_comment_author_id": 0,
        "issue_comment_author_login": "",
        "issue_comment_author_type": "",

        "pull_commits": 0,
        "pull_additions": 0,
        "pull_deletions": 0,
        "pull_changed_files": 0,
        "pull_merged": 0,
        "pull_merged_commit_sha": "",
        "pull_merged_at": "1970-01-01 00:00:00",
        "pull_merged_by_id": 0,
        "pull_merged_by_login": "",
        "pull_merged_by_type": "",
        "pull_requested_reviewer_id": 0,
        "pull_requested_reviewer_login": "",
        "pull_requested_reviewer_type": "",
        "pull_review_comments": 0,

        "repo_description": "",
        "repo_size": 0,
        "repo_stargazers_count": 0,
        "repo_forks_count": 0,
        "repo_language": "",
        "repo_has_issues": 0,
        "repo_has_projects": 0,
        "repo_has_downloads": 0,
        "repo_has_wiki": 0,
        "repo_has_pages": 0,
        "repo_license": "",
        "repo_default_branch": "",
        "repo_created_at": "1970-01-01 00:00:00",
        "repo_updated_at": "1970-01-01 00:00:00",
        "repo_pushed_at": "1970-01-01 00:00:00",

        "pull_review_id": 0,
        "pull_review_comment_id": 0,
        "pull_review_comment_path": "",
        "pull_review_comment_position": "",
        "pull_review_comment_author_id": 0,
        "pull_review_comment_author_login": "",
        "pull_review_comment_author_type": "",
        "pull_review_comment_author_association": "",
        "pull_review_comment_body": "",
        "pull_review_comment_created_at": "1970-01-01 00:00:00",
        "pull_review_comment_updated_at": "1970-01-01 00:00:00",

        # Nested columns must be objects of arrays for JSONEachRow.
        "issue_labels": {"name": [], "color": [], "default": [], "description": []},
        "issue_assignees": {"id": [], "login": []},

        # Keep other nested columns empty (not used here)
        "push_commits": {"name": [], "email": [], "message": []},
        "gollum_pages": {"page_name": [], "title": [], "action": []},
        "release_assets": {"name": [], "uploader_id": [], "uploader_login": [], "content_type": [], "state": [], "size": [], "download_count": []},
    }

def write_row(f_out, row: Dict[str, Any]) -> None:
    json.dump(row, f_out, ensure_ascii=False)
    f_out.write("\n")

# =========================
# GITHUB DATA FETCH
# =========================

def get_org_repos(org: str) -> List[str]:
    url = f"https://api.github.com/orgs/{org}/repos"
    repos = []
    page = 1
    print(f"--- Fetching repositories for org: {org} ---")
    while True:
        params = {"per_page": PER_PAGE, "page": page, "type": "all", "sort": "full_name", "direction": "asc"}
        status, data, _ = request_json(url, params)
        if status != 200 or not data:
            break
        for r in data:
            repos.append(r["name"])
        if len(data) < PER_PAGE:
            break
        page += 1
    return repos

def get_repo_meta(owner: str, repo: str) -> Dict[str, Any]:
    url = f"https://api.github.com/repos/{owner}/{repo}"
    status, data, _ = request_json(url, None)
    if status != 200 or not data:
        return {}
    lic = data.get("license") or {}
    owner_obj = data.get("owner") or {}
    return {
        "repo_id": int(data.get("id") or 0),
        "repo_name": data.get("full_name") or f"{owner}/{repo}",
        "repo_description": data.get("description") or "",
        "repo_size": int(data.get("size") or 0),
        "repo_stargazers_count": int(data.get("stargazers_count") or 0),
        "repo_forks_count": int(data.get("forks_count") or 0),
        "repo_language": data.get("language") or "",
        "repo_has_issues": 1 if data.get("has_issues") else 0,
        "repo_has_projects": 1 if data.get("has_projects") else 0,
        "repo_has_downloads": 1 if data.get("has_downloads") else 0,
        "repo_has_wiki": 1 if data.get("has_wiki") else 0,
        "repo_has_pages": 1 if data.get("has_pages") else 0,
        "repo_license": lic.get("spdx_id") or lic.get("key") or "",
        "repo_default_branch": data.get("default_branch") or "",
        "repo_created_at": iso_to_ch_datetime(data.get("created_at")),
        "repo_updated_at": iso_to_ch_datetime(data.get("updated_at")),
        "repo_pushed_at": iso_to_ch_datetime(data.get("pushed_at")),
        # org fields (basic)
        "org_id": int(owner_obj.get("id") or 0) if (owner_obj.get("type") == "Organization") else 0,
        "org_login": owner_obj.get("login") or "",
    }

def fetch_issues_opened(owner: str, repo: str) -> List[Dict[str, Any]]:
    """IssuesEvent opened (issues only, no PRs)"""
    url = f"https://api.github.com/repos/{owner}/{repo}/issues"
    rows = []
    page = 1
    while True:
        params = {"per_page": PER_PAGE, "page": page, "state": "all", "sort": "created", "direction": "desc"}
        status, data, _ = request_json(url, params)
        if status != 200 or not data:
            break

        stop_now = False
        for issue in data:
            if "pull_request" in issue:
                continue  # skip PRs in /issues
            created_iso = issue.get("created_at")
            if not created_iso:
                continue
            if not dt_ge_stop(created_iso):
                stop_now = True
                continue

            row = default_row()
            row["id"] = f"issue_open:{issue.get('id', issue.get('number', ''))}"
            row["type"] = "IssuesEvent"
            row["action"] = "opened"

            user = issue.get("user") or {}
            row["actor_id"] = int(user.get("id") or 0)
            row["actor_login"] = user.get("login") or ""

            row["created_at"] = iso_to_ch_datetime(created_iso)
            row["created_date"] = iso_to_ch_date(created_iso)

            row["issue_id"] = int(issue.get("id") or 0)
            row["issue_number"] = int(issue.get("number") or 0)
            row["issue_title"] = issue.get("title") or ""
            row["issue_body"] = issue.get("body") or ""

            row["issue_author_id"] = int(user.get("id") or 0)
            row["issue_author_login"] = user.get("login") or ""
            row["issue_author_type"] = user.get("type") or ""
            row["issue_author_association"] = issue.get("author_association") or ""

            assignee = issue.get("assignee") or {}
            row["issue_assignee_id"] = int(assignee.get("id") or 0) if assignee else 0
            row["issue_assignee_login"] = assignee.get("login") or ""

            row["issue_created_at"] = iso_to_ch_datetime(issue.get("created_at"))
            row["issue_updated_at"] = iso_to_ch_datetime(issue.get("updated_at"))
            row["issue_closed_at"] = iso_to_ch_datetime(issue.get("closed_at"))
            row["issue_comments"] = int(issue.get("comments") or 0)

            # labels nested
            labels = issue.get("labels") or []
            row["issue_labels"] = {
                "name": [l.get("name") or "" for l in labels],
                "color": [l.get("color") or "" for l in labels],
                "default": [1 if l.get("default") else 0 for l in labels],
                "description": [l.get("description") or "" for l in labels],
            }

            # assignees nested
            assignees = issue.get("assignees") or []
            row["issue_assignees"] = {
                "id": [int(a.get("id") or 0) for a in assignees],
                "login": [a.get("login") or "" for a in assignees],
            }

            rows.append(row)

        if stop_now:
            break
        if len(data) < PER_PAGE:
            break
        page += 1

    return rows

def fetch_issue_comments(owner: str, repo: str, issue_cache: Dict[int, Dict[str, Any]]) -> List[Dict[str, Any]]:
    """IssueCommentEvent created (comments for issues and PRs)."""
    url = f"https://api.github.com/repos/{owner}/{repo}/issues/comments"
    rows = []
    page = 1
    while True:
        params = {"per_page": PER_PAGE, "page": page, "sort": "created", "direction": "desc"}
        status, data, _ = request_json(url, params)
        if status != 200 or not data:
            break

        stop_now = False
        for c in data:
            created_iso = c.get("created_at")
            if not created_iso:
                continue
            if not dt_ge_stop(created_iso):
                stop_now = True
                continue

            issue_url = c.get("issue_url") or ""
            issue_number = parse_issue_number_from_url(issue_url) or 0

            # Try to enrich with issue data (optional but helps fill issue_id/title/body)
            if issue_number and issue_number not in issue_cache:
                i_url = f"https://api.github.com/repos/{owner}/{repo}/issues/{issue_number}"
                s, issue_obj, _ = request_json(i_url, None)
                if s == 200 and issue_obj:
                    issue_cache[issue_number] = issue_obj

            issue_obj = issue_cache.get(issue_number, {})

            row = default_row()
            row["id"] = f"issue_comment:{c.get('id', '')}"
            row["type"] = "IssueCommentEvent"
            row["action"] = "created"

            user = c.get("user") or {}
            row["actor_id"] = int(user.get("id") or 0)
            row["actor_login"] = user.get("login") or ""

            row["created_at"] = iso_to_ch_datetime(created_iso)
            row["created_date"] = iso_to_ch_date(created_iso)

            row["issue_number"] = int(issue_number or 0)
            row["issue_id"] = int(issue_obj.get("id") or 0)
            row["issue_title"] = issue_obj.get("title") or ""
            row["issue_body"] = issue_obj.get("body") or ""

            row["issue_comment_id"] = int(c.get("id") or 0)
            row["issue_comment_body"] = c.get("body") or ""
            row["issue_comment_created_at"] = iso_to_ch_datetime(c.get("created_at"))
            row["issue_comment_updated_at"] = iso_to_ch_datetime(c.get("updated_at"))
            row["issue_comment_author_association"] = c.get("author_association") or ""
            row["issue_comment_author_id"] = int(user.get("id") or 0)
            row["issue_comment_author_login"] = user.get("login") or ""
            row["issue_comment_author_type"] = user.get("type") or ""

            rows.append(row)

        if stop_now:
            break
        if len(data) < PER_PAGE:
            break
        page += 1
    return rows

def fetch_pulls_opened_and_merged(owner: str, repo: str) -> Tuple[List[Dict[str, Any]], List[int]]:
    """PullRequestEvent opened + PullRequestEvent merged (as closed+merged=1).
       Returns rows + list of PR numbers that are in window (for review-comment fetching).
    """
    url = f"https://api.github.com/repos/{owner}/{repo}/pulls"
    rows = []
    pr_numbers_in_window: List[int] = []
    page = 1
    while True:
        params = {"per_page": PER_PAGE, "page": page, "state": "all", "sort": "created", "direction": "desc"}
        status, data, _ = request_json(url, params)
        if status != 200 or not data:
            break

        stop_now = False
        for pr in data:
            pr_number = int(pr.get("number") or 0)
            created_iso = pr.get("created_at")
            merged_iso = pr.get("merged_at")  # may be null in list response
            # We use created_at to stop paginating backward
            if created_iso and not dt_ge_stop(created_iso):
                stop_now = True
                continue

            # Put PR number for review-comments fetching if PR is in time window by created_at
            if created_iso and dt_ge_stop(created_iso):
                pr_numbers_in_window.append(pr_number)

            # Fetch full PR details to reliably fill stats + merged_by + merged_at
            pr_full = pr
            pr_url = f"https://api.github.com/repos/{owner}/{repo}/pulls/{pr_number}"
            s, pr_full_data, _ = request_json(pr_url, None)
            if s == 200 and pr_full_data:
                pr_full = pr_full_data
                merged_iso = pr_full.get("merged_at")

            # OPENED event (created_at in window)
            if created_iso and dt_ge_stop(created_iso):
                row = default_row()
                row["id"] = f"pr_open:{pr_full.get('id', pr_number)}"
                row["type"] = "PullRequestEvent"
                row["action"] = "opened"

                user = pr_full.get("user") or {}
                row["actor_id"] = int(user.get("id") or 0)
                row["actor_login"] = user.get("login") or ""

                row["created_at"] = iso_to_ch_datetime(created_iso)
                row["created_date"] = iso_to_ch_date(created_iso)

                # Put PR number into issue_number slot (common practice in GHArchive-like schemas)
                row["issue_number"] = int(pr_number or 0)

                row["pull_commits"] = int(pr_full.get("commits") or 0)
                row["pull_additions"] = int(pr_full.get("additions") or 0)
                row["pull_deletions"] = int(pr_full.get("deletions") or 0)
                row["pull_changed_files"] = int(pr_full.get("changed_files") or 0)
                row["pull_review_comments"] = int(pr_full.get("review_comments") or 0)

                # requested reviewer (table has single fields; we take the first if any)
                rr = (pr_full.get("requested_reviewers") or [])
                if rr:
                    r0 = rr[0] or {}
                    row["pull_requested_reviewer_id"] = int(r0.get("id") or 0)
                    row["pull_requested_reviewer_login"] = r0.get("login") or ""
                    row["pull_requested_reviewer_type"] = r0.get("type") or ""

                rows.append(row)

            # MERGED event (merged_at in window)
            if merged_iso and dt_ge_stop(merged_iso):
                row = default_row()
                row["id"] = f"pr_merged:{pr_full.get('id', pr_number)}"
                row["type"] = "PullRequestEvent"
                row["action"] = "closed"   # GH event semantics: merged is a closed PR with merged=true

                merged_by = pr_full.get("merged_by") or {}
                # For merged event, "actor" is often the merger (merged_by); if absent, fallback to PR author
                actor = merged_by if merged_by else (pr_full.get("user") or {})
                row["actor_id"] = int(actor.get("id") or 0)
                row["actor_login"] = actor.get("login") or ""

                row["created_at"] = iso_to_ch_datetime(merged_iso)
                row["created_date"] = iso_to_ch_date(merged_iso)

                row["issue_number"] = int(pr_number or 0)

                row["pull_merged"] = 1
                row["pull_merged_at"] = iso_to_ch_datetime(merged_iso)
                row["pull_merged_commit_sha"] = pr_full.get("merge_commit_sha") or ""
                row["pull_merged_by_id"] = int(merged_by.get("id") or 0)
                row["pull_merged_by_login"] = merged_by.get("login") or ""
                row["pull_merged_by_type"] = merged_by.get("type") or ""

                rows.append(row)

        if stop_now:
            break
        if len(data) < PER_PAGE:
            break
        page += 1

    # de-dup PR numbers
    pr_numbers_in_window = sorted(set([n for n in pr_numbers_in_window if n > 0]))
    return rows, pr_numbers_in_window

def fetch_pr_review_comments(owner: str, repo: str, pr_numbers: List[int]) -> List[Dict[str, Any]]:
    """PullRequestReviewCommentEvent created (per-PR endpoint)."""
    rows = []
    for pr_number in pr_numbers:
        url = f"https://api.github.com/repos/{owner}/{repo}/pulls/{pr_number}/comments"
        page = 1
        while True:
            params = {"per_page": PER_PAGE, "page": page, "sort": "created", "direction": "desc"}
            status, data, _ = request_json(url, params)
            if status != 200 or not data:
                break

            stop_now = False
            for c in data:
                created_iso = c.get("created_at")
                if not created_iso:
                    continue
                if not dt_ge_stop(created_iso):
                    stop_now = True
                    continue

                user = c.get("user") or {}
                row = default_row()
                row["id"] = f"pr_review_comment:{c.get('id', '')}"
                row["type"] = "PullRequestReviewCommentEvent"
                row["action"] = "created"

                row["actor_id"] = int(user.get("id") or 0)
                row["actor_login"] = user.get("login") or ""
                row["created_at"] = iso_to_ch_datetime(created_iso)
                row["created_date"] = iso_to_ch_date(created_iso)

                row["issue_number"] = int(pr_number or 0)

                row["pull_review_id"] = int(c.get("pull_request_review_id") or 0)
                row["pull_review_comment_id"] = int(c.get("id") or 0)
                row["pull_review_comment_path"] = c.get("path") or ""
                # position can be int or null; your column is String, so store as string
                pos = c.get("position")
                row["pull_review_comment_position"] = "" if pos is None else str(pos)

                row["pull_review_comment_author_id"] = int(user.get("id") or 0)
                row["pull_review_comment_author_login"] = user.get("login") or ""
                row["pull_review_comment_author_type"] = user.get("type") or ""
                row["pull_review_comment_author_association"] = c.get("author_association") or ""

                row["pull_review_comment_body"] = c.get("body") or ""
                row["pull_review_comment_created_at"] = iso_to_ch_datetime(c.get("created_at"))
                row["pull_review_comment_updated_at"] = iso_to_ch_datetime(c.get("updated_at"))

                rows.append(row)

            if stop_now:
                break
            if len(data) < PER_PAGE:
                break
            page += 1

    return rows

# =========================
# MAIN ETL
# =========================

def main():
    if not GITHUB_TOKEN or GITHUB_TOKEN == "your_token_here":
        print("Please set GITHUB_TOKEN environment variable.")
        return

    all_repos = get_org_repos(ORG_NAME)
    print(f"Found {len(all_repos)} repositories in {ORG_NAME}.")
    seen_ids = set()

    with gzip.open(OUTPUT_FILE, "wt", encoding="utf-8") as f_out:
        for repo_name in all_repos:
            owner = ORG_NAME
            repo = repo_name
            full = f"{owner}/{repo}"
            print(f"\n>>> PROCESSING REPO: {full} <<<")

            try:
                print(f"repo metadata...")
                repo_meta = get_repo_meta(owner, repo)
                # Fallback org fields if repo owner type not org
                if not repo_meta.get("org_login"):
                    repo_meta["org_login"] = ORG_NAME

                issue_cache: Dict[int, Dict[str, Any]] = {}

                # 1) Issues opened
                print(f"issues opened...")
                issue_rows = fetch_issues_opened(owner, repo)

                # 2) Issue comments (issues + PR comments)
                print(f"issue comments...")
                comment_rows = fetch_issue_comments(owner, repo, issue_cache)

                # 3) PR opened + merged (+ collect PR numbers for review comments)
                print(f"pr opened and merged...")
                pr_rows, pr_numbers = fetch_pulls_opened_and_merged(owner, repo)

                # 4) PR review comments
                print(f"review comments...")
                pr_review_comment_rows = fetch_pr_review_comments(owner, repo, pr_numbers)

                # Merge and enrich with repo/org metadata
                all_rows = issue_rows + comment_rows + pr_rows + pr_review_comment_rows

                for row in all_rows:
                    # inject repo/org meta into every row
                    row["repo_id"] = int(repo_meta.get("repo_id") or 0)
                    row["repo_name"] = repo_meta.get("repo_name") or full
                    row["org_id"] = int(repo_meta.get("org_id") or 0)
                    row["org_login"] = repo_meta.get("org_login") or ORG_NAME

                    row["repo_description"] = repo_meta.get("repo_description") or ""
                    row["repo_size"] = int(repo_meta.get("repo_size") or 0)
                    row["repo_stargazers_count"] = int(repo_meta.get("repo_stargazers_count") or 0)
                    row["repo_forks_count"] = int(repo_meta.get("repo_forks_count") or 0)
                    row["repo_language"] = repo_meta.get("repo_language") or ""
                    row["repo_has_issues"] = int(repo_meta.get("repo_has_issues") or 0)
                    row["repo_has_projects"] = int(repo_meta.get("repo_has_projects") or 0)
                    row["repo_has_downloads"] = int(repo_meta.get("repo_has_downloads") or 0)
                    row["repo_has_wiki"] = int(repo_meta.get("repo_has_wiki") or 0)
                    row["repo_has_pages"] = int(repo_meta.get("repo_has_pages") or 0)
                    row["repo_license"] = repo_meta.get("repo_license") or ""
                    row["repo_default_branch"] = repo_meta.get("repo_default_branch") or ""
                    row["repo_created_at"] = repo_meta.get("repo_created_at") or "1970-01-01 00:00:00"
                    row["repo_updated_at"] = repo_meta.get("repo_updated_at") or "1970-01-01 00:00:00"
                    row["repo_pushed_at"] = repo_meta.get("repo_pushed_at") or "1970-01-01 00:00:00"

                    # de-dup by row["id"]
                    rid = row.get("id") or ""
                    if not rid or rid in seen_ids:
                        continue
                    seen_ids.add(rid)

                    write_row(f_out, row)

                print(f"  Wrote {len(all_rows)} rows (before de-dup) for {full}. PRs in window: {len(pr_numbers)}")

            except Exception as e:
                print(f"Error processing {full}: {e}")

    print(f"\nFinished. Data saved to {OUTPUT_FILE}")
    print(f"Unique rows written: {len(seen_ids)}")

if __name__ == "__main__":
    main()

