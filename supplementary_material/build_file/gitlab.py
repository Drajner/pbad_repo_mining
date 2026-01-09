import os
import json
import gzip
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import requests

# =========================
# CONFIG
# =========================
GITLAB_BASE_URL = os.getenv("GITLAB_BASE_URL", "https://gitlab.com")  # self-managed: https://gitlab.example.com
GITLAB_TOKEN = os.getenv("GITLAB_TOKEN", "your_token_here")

# Group identifier: can be numeric ID or URL-encoded full path (e.g. "my-group%2Fsubgroup")
GROUP_ID_OR_PATH = os.getenv("GITLAB_GROUP", "WiseLibs")

OUTPUT_FILE = "gitlab_group_activity_events.json.gz"

# Include items with created_at >= STOP_DATE
STOP_DATE = datetime(2024, 1, 1, tzinfo=timezone.utc)

PER_PAGE = 100
REQUEST_TIMEOUT = 30

HEADERS = {
    "PRIVATE-TOKEN": GITLAB_TOKEN,
    "User-Agent": "gitlab-activity-etl",
}

# =========================
# TIME HELPERS
# =========================
def parse_iso(iso_str: Optional[str]) -> Optional[datetime]:
    if not iso_str:
        return None
    # GitLab returns ISO8601 with Z or timezone offset
    s = iso_str.replace("Z", "+00:00")
    return datetime.fromisoformat(s).astimezone(timezone.utc)

def dt_ge_stop(iso_str: str) -> bool:
    dt = parse_iso(iso_str)
    return bool(dt and dt >= STOP_DATE)

def iso_to_ch_datetime(iso_str: Optional[str]) -> str:
    dt = parse_iso(iso_str)
    return dt.strftime("%Y-%m-%d %H:%M:%S") if dt else "1970-01-01 00:00:00"

def iso_to_ch_date(iso_str: Optional[str]) -> str:
    dt = parse_iso(iso_str)
    return dt.strftime("%Y-%m-%d") if dt else "1970-01-01"

# =========================
# HTTP HELPERS (retries + basic backoff)
# =========================
def request_json(url: str, params: Optional[dict] = None) -> Tuple[int, Any, Dict[str, str]]:
    backoff = 1.0
    for attempt in range(8):
        resp = requests.get(url, headers=HEADERS, params=params, timeout=REQUEST_TIMEOUT)

        # GitLab throttling: sometimes 429
        if resp.status_code == 429:
            retry_after = resp.headers.get("Retry-After")
            sleep_for = int(retry_after) if retry_after and retry_after.isdigit() else int(backoff)
            print(f"429 Too Many Requests. Sleeping {sleep_for}s...")
            time.sleep(sleep_for)
            backoff = min(backoff * 2, 30)
            continue

        if resp.status_code in (502, 503, 504, 522, 524):
            print(f"Transient error {resp.status_code} on {url}. Backoff {backoff}s (attempt {attempt+1})")
            time.sleep(backoff)
            backoff = min(backoff * 2, 30)
            continue

        try:
            data = resp.json() if resp.content else None
        except Exception:
            data = None
        return resp.status_code, data, dict(resp.headers)

    return 599, None, {}

def paginate(url: str, base_params: Optional[dict] = None) -> List[Any]:
    out: List[Any] = []
    page = 1
    base_params = base_params or {}
    while True:
        params = dict(base_params)
        params["per_page"] = PER_PAGE
        params["page"] = page
        status, data, _ = request_json(url, params)
        if status != 200 or not data:
            break
        out.extend(data)
        if len(data) < PER_PAGE:
            break
        page += 1
    return out

# =========================
# CLICKHOUSE ROW DEFAULTS (subset used)
# =========================
def default_row() -> Dict[str, Any]:
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

        "issue_labels": {"name": [], "color": [], "default": [], "description": []},
        "issue_assignees": {"id": [], "login": []},

        "push_commits": {"name": [], "email": [], "message": []},
        "gollum_pages": {"page_name": [], "title": [], "action": []},
        "release_assets": {"name": [], "uploader_id": [], "uploader_login": [], "content_type": [], "state": [], "size": [], "download_count": []},
    }

def write_row(f_out, row: Dict[str, Any]) -> None:
    json.dump(row, f_out, ensure_ascii=False)
    f_out.write("\n")

# =========================
# GITLAB FETCH
# =========================
def api_url(path: str) -> str:
    return f"{GITLAB_BASE_URL.rstrip('/')}/api/v4{path}"

def get_group(group_id_or_path: str) -> Dict[str, Any]:
    # Group can be numeric ID or URL-encoded path
    url = api_url(f"/groups/{group_id_or_path}")
    status, data, _ = request_json(url)
    return data if status == 200 and data else {}

def get_group_projects(group_id_or_path: str) -> List[Dict[str, Any]]:
    url = api_url(f"/groups/{group_id_or_path}/projects")
    # include_subgroups=true is handy for nested groups
    params = {"include_subgroups": "true", "with_shared": "false", "simple": "false", "order_by": "id", "sort": "asc"}
    return paginate(url, params)

def get_project(project_id: int) -> Dict[str, Any]:
    url = api_url(f"/projects/{project_id}")
    status, data, _ = request_json(url)
    return data if status == 200 and data else {}

def project_meta_to_repo_fields(p: Dict[str, Any]) -> Dict[str, Any]:
    # Map GitLab project -> your repo_* columns (best-effort)
    return {
        "repo_id": int(p.get("id") or 0),
        "repo_name": p.get("path_with_namespace") or "",
        "repo_description": p.get("description") or "",
        "repo_size": 0,  # GitLab doesn't expose size in the same simple field
        "repo_stargazers_count": int(p.get("star_count") or 0),
        "repo_forks_count": int(p.get("forks_count") or 0),
        "repo_language": "",  # GitLab needs languages endpoint if you want it (optional)
        "repo_has_issues": 1 if p.get("issues_enabled") else 0,
        "repo_has_projects": 1,  # not applicable; keep 1 or 0 depending on your convention
        "repo_has_downloads": 0,  # GitLab: packages/releases differ
        "repo_has_wiki": 1 if p.get("wiki_enabled") else 0,
        "repo_has_pages": 1 if p.get("pages_access_level") and p.get("pages_access_level") != "disabled" else 0,
        "repo_license": "",  # GitLab license endpoint exists; keep empty unless you need it
        "repo_default_branch": p.get("default_branch") or "",
        "repo_created_at": iso_to_ch_datetime(p.get("created_at")),
        "repo_updated_at": iso_to_ch_datetime(p.get("last_activity_at")),  # closest analog
        "repo_pushed_at": iso_to_ch_datetime(p.get("last_activity_at")),
    }

def fetch_issues_opened(project_id: int) -> List[Dict[str, Any]]:
    url = api_url(f"/projects/{project_id}/issues")
    params = {"state": "all", "order_by": "created_at", "sort": "desc"}
    issues = paginate(url, params)

    rows: List[Dict[str, Any]] = []
    for issue in issues:
        created_iso = issue.get("created_at")
        if not created_iso:
            continue
        if not dt_ge_stop(created_iso):
            # issues are sorted desc by created_at => can stop early
            break

        author = issue.get("author") or {}
        assignees = issue.get("assignees") or []
        first_assignee = assignees[0] if assignees else {}

        row = default_row()
        row["id"] = f"gl_issue_open:{project_id}:{issue.get('id', issue.get('iid', ''))}"
        row["type"] = "IssuesEvent"
        row["action"] = "opened"

        row["actor_id"] = int(author.get("id") or 0)
        row["actor_login"] = author.get("username") or ""

        row["created_at"] = iso_to_ch_datetime(created_iso)
        row["created_date"] = iso_to_ch_date(created_iso)

        row["issue_id"] = int(issue.get("id") or 0)
        row["issue_number"] = int(issue.get("iid") or 0)
        row["issue_title"] = issue.get("title") or ""
        row["issue_body"] = issue.get("description") or ""

        row["issue_author_id"] = int(author.get("id") or 0)
        row["issue_author_login"] = author.get("username") or ""
        row["issue_author_type"] = "User"  # GitLab doesn't expose type like GitHub; keep constant
        row["issue_author_association"] = ""  # could be derived from memberships; omit

        row["issue_assignee_id"] = int(first_assignee.get("id") or 0) if first_assignee else 0
        row["issue_assignee_login"] = first_assignee.get("username") or ""

        row["issue_created_at"] = iso_to_ch_datetime(issue.get("created_at"))
        row["issue_updated_at"] = iso_to_ch_datetime(issue.get("updated_at"))
        row["issue_closed_at"] = iso_to_ch_datetime(issue.get("closed_at"))
        row["issue_comments"] = int(issue.get("user_notes_count") or 0)

        labels = issue.get("labels") or []
        row["issue_labels"] = {
            "name": [str(l) for l in labels],
            "color": ["" for _ in labels],
            "default": [0 for _ in labels],
            "description": ["" for _ in labels],
        }
        row["issue_assignees"] = {
            "id": [int(a.get("id") or 0) for a in assignees],
            "login": [a.get("username") or "" for a in assignees],
        }

        rows.append(row)

    return rows

def fetch_issue_comments(project_id: int, issue_iid: int) -> List[Dict[str, Any]]:
    # Notes API: activity_filter=only_comments filters out system notes
    url = api_url(f"/projects/{project_id}/issues/{issue_iid}/notes")
    params = {"sort": "desc", "order_by": "created_at", "activity_filter": "only_comments"}
    notes = paginate(url, params)

    rows: List[Dict[str, Any]] = []
    for n in notes:
        created_iso = n.get("created_at")
        if not created_iso:
            continue
        if not dt_ge_stop(created_iso):
            break

        author = n.get("author") or {}
        row = default_row()
        row["id"] = f"gl_issue_comment:{project_id}:{issue_iid}:{n.get('id','')}"
        row["type"] = "IssueCommentEvent"
        row["action"] = "created"

        row["actor_id"] = int(author.get("id") or 0)
        row["actor_login"] = author.get("username") or ""

        row["created_at"] = iso_to_ch_datetime(created_iso)
        row["created_date"] = iso_to_ch_date(created_iso)

        row["issue_number"] = int(issue_iid)
        # issue_id/title/body not present in note; zostaw 0/'' (można dociągać issue, jeśli chcesz)

        row["issue_comment_id"] = int(n.get("id") or 0)
        row["issue_comment_body"] = n.get("body") or ""
        row["issue_comment_created_at"] = iso_to_ch_datetime(n.get("created_at"))
        row["issue_comment_updated_at"] = iso_to_ch_datetime(n.get("updated_at"))

        row["issue_comment_author_association"] = ""
        row["issue_comment_author_id"] = int(author.get("id") or 0)
        row["issue_comment_author_login"] = author.get("username") or ""
        row["issue_comment_author_type"] = "User"

        rows.append(row)

    return rows

def fetch_merge_requests_opened_and_merged(project_id: int) -> Tuple[List[Dict[str, Any]], List[int]]:
    url = api_url(f"/projects/{project_id}/merge_requests")
    params = {"state": "all", "order_by": "created_at", "sort": "desc"}
    mrs = paginate(url, params)

    rows: List[Dict[str, Any]] = []
    mr_iids_in_window: List[int] = []

    for mr in mrs:
        created_iso = mr.get("created_at")
        if not created_iso:
            continue
        if not dt_ge_stop(created_iso):
            break

        mr_iid = int(mr.get("iid") or 0)
        if mr_iid:
            mr_iids_in_window.append(mr_iid)

        author = mr.get("author") or {}

        # OPENED
        row_open = default_row()
        row_open["id"] = f"gl_mr_open:{project_id}:{mr.get('id', mr_iid)}"
        row_open["type"] = "PullRequestEvent"
        row_open["action"] = "opened"
        row_open["actor_id"] = int(author.get("id") or 0)
        row_open["actor_login"] = author.get("username") or ""
        row_open["created_at"] = iso_to_ch_datetime(created_iso)
        row_open["created_date"] = iso_to_ch_date(created_iso)
        row_open["issue_number"] = mr_iid  # MR iid into issue_number slot
        # stats fields remain 0 unless you later enrich via additional endpoints
        rows.append(row_open)

        # MERGED (if merged_at exists and is in window)
        merged_iso = mr.get("merged_at")
        if merged_iso and dt_ge_stop(merged_iso):
            merged_by = mr.get("merged_by") or {}
            actor = merged_by if merged_by else author

            row_m = default_row()
            row_m["id"] = f"gl_mr_merged:{project_id}:{mr.get('id', mr_iid)}"
            row_m["type"] = "PullRequestEvent"
            row_m["action"] = "closed"
            row_m["actor_id"] = int(actor.get("id") or 0)
            row_m["actor_login"] = actor.get("username") or ""
            row_m["created_at"] = iso_to_ch_datetime(merged_iso)
            row_m["created_date"] = iso_to_ch_date(merged_iso)

            row_m["issue_number"] = mr_iid
            row_m["pull_merged"] = 1
            row_m["pull_merged_at"] = iso_to_ch_datetime(merged_iso)
            row_m["pull_merged_commit_sha"] = mr.get("merge_commit_sha") or ""
            row_m["pull_merged_by_id"] = int(merged_by.get("id") or 0)
            row_m["pull_merged_by_login"] = merged_by.get("username") or ""
            row_m["pull_merged_by_type"] = "User" if merged_by else ""

            rows.append(row_m)

    mr_iids_in_window = sorted(set([x for x in mr_iids_in_window if x > 0]))
    return rows, mr_iids_in_window

def fetch_mr_review_comments_as_discussions(project_id: int, mr_iid: int) -> List[Dict[str, Any]]:
    url = api_url(f"/projects/{project_id}/merge_requests/{mr_iid}/discussions")
    params = {"sort": "desc", "order_by": "created_at"}
    discussions = paginate(url, params)

    rows: List[Dict[str, Any]] = []
    for d in discussions:
        notes = d.get("notes") or []
        for n in notes:
            # We want actual user comments; system notes may exist
            if n.get("system"):
                continue

            created_iso = n.get("created_at")
            if not created_iso:
                continue
            if not dt_ge_stop(created_iso):
                # discussions are not guaranteed to be strictly ordered by note created_at,
                # so we cannot safely break globally; just continue.
                continue

            author = n.get("author") or {}
            row = default_row()
            row["id"] = f"gl_mr_review_comment:{project_id}:{mr_iid}:{n.get('id','')}"
            row["type"] = "PullRequestReviewCommentEvent"
            row["action"] = "created"

            row["actor_id"] = int(author.get("id") or 0)
            row["actor_login"] = author.get("username") or ""
            row["created_at"] = iso_to_ch_datetime(created_iso)
            row["created_date"] = iso_to_ch_date(created_iso)
            row["issue_number"] = mr_iid

            row["pull_review_id"] = 0  # GitLab discussions don't map 1:1 to "review_id"
            row["pull_review_comment_id"] = int(n.get("id") or 0)

            # If it's a diff note, GitLab provides position info under n["position"]
            pos = n.get("position") or {}
            row["pull_review_comment_path"] = pos.get("new_path") or pos.get("old_path") or ""
            # There isn't a single "position" like GitHub; store line number as string if available
            line = pos.get("new_line") or pos.get("old_line")
            row["pull_review_comment_position"] = "" if line is None else str(line)

            row["pull_review_comment_author_id"] = int(author.get("id") or 0)
            row["pull_review_comment_author_login"] = author.get("username") or ""
            row["pull_review_comment_author_type"] = "User"
            row["pull_review_comment_author_association"] = ""

            row["pull_review_comment_body"] = n.get("body") or ""
            row["pull_review_comment_created_at"] = iso_to_ch_datetime(n.get("created_at"))
            row["pull_review_comment_updated_at"] = iso_to_ch_datetime(n.get("updated_at"))

            rows.append(row)

    return rows

# =========================
# MAIN
# =========================
def main():
    if not GITLAB_TOKEN or GITLAB_TOKEN == "your_token_here":
        print("Please set GITLAB_TOKEN env var.")
        return

    group = get_group(GROUP_ID_OR_PATH)
    org_id = int(group.get("id") or 0)
    org_login = group.get("full_path") or group.get("path") or str(GROUP_ID_OR_PATH)

    print(f"Group resolved: org_id={org_id}, org_login={org_login}")

    projects = get_group_projects(GROUP_ID_OR_PATH)
    print(f"Found {len(projects)} projects in group {org_login}.")

    seen_ids = set()

    with gzip.open(OUTPUT_FILE, "wt", encoding="utf-8") as f_out:
        for p in projects:
            project_id = int(p.get("id") or 0)
            if not project_id:
                continue

            # Fetch full project to get richer metadata
            proj_full = get_project(project_id) or p
            repo_meta = project_meta_to_repo_fields(proj_full)

            print(f"\n>>> PROJECT: {repo_meta.get('repo_name','')} (id={project_id}) <<<")

            # 1) Issues opened
            issue_rows = fetch_issues_opened(project_id)

            # 2) Issue comments (notes)
            comment_rows: List[Dict[str, Any]] = []
            # Only for issues in-window to reduce calls
            for r in issue_rows:
                issue_iid = int(r.get("issue_number") or 0)
                if issue_iid:
                    comment_rows.extend(fetch_issue_comments(project_id, issue_iid))

            # 3) Merge requests opened + merged
            mr_rows, mr_iids = fetch_merge_requests_opened_and_merged(project_id)

            # 4) MR review comments (discussions)
            mr_review_comment_rows: List[Dict[str, Any]] = []
            for mr_iid in mr_iids:
                mr_review_comment_rows.extend(fetch_mr_review_comments_as_discussions(project_id, mr_iid))

            all_rows = issue_rows + comment_rows + mr_rows + mr_review_comment_rows

            # Enrich + write
            for row in all_rows:
                # org
                row["org_id"] = org_id
                row["org_login"] = org_login

                # repo meta
                row["repo_id"] = int(repo_meta.get("repo_id") or 0)
                row["repo_name"] = repo_meta.get("repo_name") or ""
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

                rid = row.get("id") or ""
                if not rid or rid in seen_ids:
                    continue
                seen_ids.add(rid)

                write_row(f_out, row)

            print(
                f"  Rows (pre-dedup): {len(all_rows)} | "
                f"Issues: {len(issue_rows)} | Issue notes: {len(comment_rows)} | "
                f"MR events: {len(mr_rows)} | MR discussions notes: {len(mr_review_comment_rows)}"
            )

    print(f"\nFinished. Data saved to {OUTPUT_FILE}")
    print(f"Unique rows written: {len(seen_ids)}")


if __name__ == "__main__":
    main()
