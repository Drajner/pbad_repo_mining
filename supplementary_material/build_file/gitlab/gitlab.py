import gzip
import hashlib
import json
import time
from datetime import datetime, timezone
from typing import Callable, Optional, Any, Dict, List, Tuple
from zoneinfo import ZoneInfo
import requests
import urllib.parse

# CONFIG ==================
GITLAB_TOKEN = "glpat-vcgLmbgSudidk2dhFyuKum86MQp1OmpxNmY0Cw.01.1213p4b8a"
GITLAB_BASE_URL = "https://gitlab.com/api/v4"
GROUP_PATH = "grafana"
EXCLUDED_PROJECTS = [
    # "grafana/grafana",
    # "grafana/k6",
    # "grafana/loki",
    # "grafana/grafana-image-renderer",
]
PROJECT_NUMBER_LIMIT = 2
OUTPUT_FILE = "repo_mining_wynik.json.gz"
STOP_DATE = datetime(2025, 1, 14, tzinfo=timezone.utc)
# ===========================

PER_PAGE = 100
REQUEST_TIMEOUT = 30

HEADERS = {
    "PRIVATE-TOKEN": GITLAB_TOKEN,
    "Accept": "application/json",
    "User-Agent": "group-activity-etl",
}

SESSION = requests.Session()
SESSION.headers.update(HEADERS)

WARSAW_TZ = ZoneInfo("Europe/Warsaw")

ANONYMIZATION_SALT = "D0NotShareThisSalt123!"
REDACTED_BODY = "xyz"

def _fmt_ts(ts: int) -> tuple[str, str]:
    dt_utc = datetime.fromtimestamp(ts, tz=timezone.utc)
    dt_pl = dt_utc.astimezone(WARSAW_TZ)
    return dt_utc.strftime("%H:%M:%S UTC"), dt_pl.strftime("%H:%M:%S %Z")

def parse_dt(iso_str: str) -> datetime:
    return datetime.fromisoformat(iso_str.replace("Z", "+00:00")).astimezone(timezone.utc)

def dt_ge_stop(iso_str: str) -> bool:
    return parse_dt(iso_str) >= STOP_DATE

def is_bot(user_data: dict) -> bool:
    if not user_data:
        return False
    if user_data.get("bot") is True:
        return True
    username = user_data.get("username") or ""
    if username and str(username).lower().endswith("[bot]"):
        return True
    name = (user_data.get("name") or "").lower()
    if "bot" == name.strip():
        return True
    return False

def get_hash_str(value: Any) -> Optional[str]:
    if value is None:
        return None
    raw = f"{ANONYMIZATION_SALT}:{value}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()

def get_hash_int(value: Any) -> Optional[int]:
    if value is None:
        return None
    raw = f"{ANONYMIZATION_SALT}:{value}"
    hex_hash = hashlib.sha256(raw.encode("utf-8")).hexdigest()
    return int(hex_hash[:13], 16)

def process_user_data(user_data: dict) -> Optional[dict]:
    if not user_data:
        return None

    if is_bot(user_data):
        return {
            "id": user_data.get("id"),
            "login": user_data.get("username"),
            "type": "Bot",
        }

    return {
        "id": get_hash_int(user_data.get("id")),
        "login": get_hash_str(user_data.get("username")),
        "type": "User",
    }


def filter_event_for_clickhouse(raw_event: dict) -> Optional[dict]:
    if not raw_event:
        return None

    filtered = {
        "id": raw_event.get("id"),
        "type": raw_event.get("type"),
        "created_at": raw_event.get("created_at"),
    }

    if "actor" in raw_event:
        filtered["actor"] = process_user_data(raw_event["actor"])

    if "repo" in raw_event:
        filtered["repo"] = {
            "id": get_hash_int(raw_event["repo"].get("id")),
            "name": get_hash_str(raw_event["repo"].get("name")),
        }

    if "org" in raw_event:
        filtered["org"] = {
            "id": get_hash_int(raw_event["org"].get("id")),
            "login": get_hash_str(raw_event["org"].get("login")),
        }

    if "payload" in raw_event:
        p = raw_event["payload"] or {}
        new_p: Dict[str, Any] = {}

        if "action" in p:
            new_p["action"] = p["action"]

        if "repository" in p and p["repository"]:
            repo_src = p["repository"]
            new_p["repository"] = {
                "description": REDACTED_BODY,
                "language": repo_src.get("language"),
                "star_count": repo_src.get("star_count"),
                "forks_count": repo_src.get("forks_count"),
                "size": repo_src.get("repository_size") or repo_src.get("size"),
                "default_branch": repo_src.get("default_branch"),
                "visibility": repo_src.get("visibility"),
            }

        if "issue" in p and p["issue"]:
            iss = p["issue"]
            new_iss = {
                "id": iss.get("id"),
                "number": iss.get("iid"),
                "title": REDACTED_BODY,
                "body": REDACTED_BODY,
                "state": iss.get("state"),
                "comments": iss.get("user_notes_count"),
            }
            if "author" in iss:
                new_iss["user"] = process_user_data(iss["author"])
            if "assignee" in iss and iss.get("assignee"):
                new_iss["assignee"] = process_user_data(iss["assignee"])
            new_p["issue"] = new_iss

        if "comment" in p and p["comment"]:
            com = p["comment"]
            new_com = {
                "id": com.get("id"),
                "body": REDACTED_BODY,
                "system": com.get("system"),
                "type": com.get("type"),
                "position": com.get("position"),
                "discussion_id": com.get("discussion_id"),
            }
            if "author" in com:
                new_com["user"] = process_user_data(com["author"])
            new_p["comment"] = new_com

        if "merge_request" in p and p["merge_request"]:
            mr = p["merge_request"]
            new_mr = {
                "id": mr.get("id"),
                "number": mr.get("iid"),
                "state": mr.get("state"),
                "merged_at": mr.get("merged_at"),
                "closed_at": mr.get("closed_at"),
                "created_at": mr.get("created_at"),
                "updated_at": mr.get("updated_at"),
                "source_branch": mr.get("source_branch"),
                "target_branch": mr.get("target_branch"),
                "title": REDACTED_BODY,
                "description": REDACTED_BODY,
            }
            if mr.get("author"):
                new_mr["author"] = process_user_data(mr["author"])
            if mr.get("merged_by"):
                new_mr["merged_by"] = process_user_data(mr["merged_by"])
            if mr.get("merge_user"):
                new_mr["merge_user"] = process_user_data(mr["merge_user"])
            new_p["merge_request"] = new_mr

        if "pull_merged" in p:
            new_p["pull_merged"] = p["pull_merged"]

        filtered["payload"] = new_p

    return filtered

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


def handle_rate_limit(resp: requests.Response) -> bool:
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
                print(f"\n[HTTP {resp.status_code}] {url} params={params} msg={j.get('message') or j.get('error')}")

            resp.raise_for_status()
            return resp

        except Exception as e:
            print(f"  [BŁĄD] {e}. Próba {attempt+1}/3... (sleep {backoff}s)")
            time.sleep(backoff)
            backoff = min(backoff * 2, 60)

    return None

def get_next_page(resp: requests.Response) -> Optional[int]:
    nxt = resp.headers.get("X-Next-Page")
    if nxt and str(nxt).isdigit():
        return int(nxt)
    return None

def iterate_items_until_stop_pages(
    url: str,
    params_base: dict,
    get_created_at: Callable[[dict], Optional[str]],
    on_item: Callable[[dict, str], None],
) -> int:
    count = 0
    page = 1

    while True:
        params = dict(params_base)
        params["per_page"] = PER_PAGE
        params["page"] = page

        resp = make_request(url, params)
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

        nxt = get_next_page(resp)
        if not nxt:
            break
        page = nxt

    return count


def get_group_metadata(group_path: str) -> dict:
    gid = urllib.parse.quote(group_path, safe="")
    url = f"{GITLAB_BASE_URL}/groups/{gid}"
    resp = make_request(url)
    if not resp:
        return {"id": 0, "login": group_path, "url": url}
    g = resp.json()
    return {
        "id": int(g.get("id") or 0),
        "login": g.get("full_path") or g.get("path") or group_path,
        "url": g.get("web_url") or "",
    }


def list_group_projects(group_path: str) -> List[dict]:
    gid = urllib.parse.quote(group_path, safe="")
    url = f"{GITLAB_BASE_URL}/groups/{gid}/projects"

    projects: List[dict] = []
    page = 1
    while True:
        params = {
            "per_page": PER_PAGE,
            "page": page,
            "include_subgroups": "true",
            "with_shared": "false",
            "order_by": "last_activity_at",
            "sort": "desc",
        }
        print(f"[LIST] projects page={page} ...", flush=True)
        resp = make_request(url, params)
        if not resp:
            break

        items = resp.json() or []
        if not items:
            break

        projects.extend(items)

        if len(projects) >= PROJECT_NUMBER_LIMIT * 2:
            break

        nxt = get_next_page(resp)
        if not nxt:
            break
        page = nxt

    return projects


def get_project(project_id: int) -> dict:
    url = f"{GITLAB_BASE_URL}/projects/{project_id}"
    resp = make_request(url)
    return resp.json() if resp else {}


def build_project_context(project: dict) -> Tuple[str, dict, dict]:
    project_id = int(project.get("id") or 0)
    full_name = project.get("path_with_namespace") or project.get("name_with_namespace") or str(project_id)

    repo_dict = {"id": project_id, "name": full_name}

    repository_payload = {
        "id": project_id,
        "name": full_name,
        "description": project.get("description") or "",
        "language": project.get("language") or "",
        "star_count": project.get("star_count"),
        "forks_count": project.get("forks_count"),
        "repository_size": project.get("repository_size"),
        "default_branch": project.get("default_branch"),
        "visibility": project.get("visibility"),
    }

    base_url = f"{GITLAB_BASE_URL}/projects/{project_id}"
    return base_url, repo_dict, repository_payload


def emit_issue_opened_events(f_out, base_url: str, repo_dict: dict, org_dict: dict, repository_payload: dict) -> int:
    url = f"{base_url}/issues"

    def on_item(it: dict, created_at: str) -> None:
        actor = (it.get("author") or {})
        payload = {"action": "opened", "issue": it, "repository": repository_payload}
        save_event(
            f_out,
            "IssuesEvent",
            created_at,
            actor,
            repo_dict,
            org_dict,
            payload,
            event_id("issue_open", it.get("id") or it.get("iid")),
        )

    return iterate_items_until_stop_pages(
        url=url,
        params_base={"state": "all", "order_by": "created_at", "sort": "desc"},
        get_created_at=lambda it: it.get("created_at"),
        on_item=on_item,
    )


def emit_issue_comment_events(f_out, base_url: str, repo_dict: dict, org_dict: dict, repository_payload: dict) -> int:
    issues_url = f"{base_url}/issues"

    issues: List[dict] = []

    def collect_issue(it: dict, created_at: str) -> None:
        issues.append(it)

    iterate_items_until_stop_pages(
        url=issues_url,
        params_base={"state": "all", "order_by": "created_at", "sort": "desc"},
        get_created_at=lambda it: it.get("created_at"),
        on_item=collect_issue,
    )

    emitted = 0

    for it in issues:
        iid = it.get("iid")
        if iid is None:
            continue
        notes_url = f"{base_url}/issues/{iid}/notes"

        def on_note(note: dict, created_at: str) -> None:
            nonlocal emitted
            actor = note.get("author") or {}
            payload = {"action": "created", "comment": note, "issue": it, "repository": repository_payload}
            save_event(
                f_out,
                "IssueCommentEvent",
                created_at,
                actor,
                repo_dict,
                org_dict,
                payload,
                event_id("issue_comment", note.get("id")),
            )
            emitted += 1

        emitted += iterate_items_until_stop_pages(
            url=notes_url,
            params_base={"order_by": "created_at", "sort": "desc"},
            get_created_at=lambda n: n.get("created_at"),
            on_item=on_note,
        )

    return emitted


def emit_merge_request_events(f_out, base_url: str, repo_dict: dict, org_dict: dict, repository_payload: dict) -> int:
    url = f"{base_url}/merge_requests"
    emitted = 0

    def on_mr_created(mr: dict, created_at: str) -> None:
        nonlocal emitted
        actor_author = mr.get("author") or {}
        payload_open = {"action": "opened", "merge_request": mr, "repository": repository_payload}
        save_event(
            f_out,
            "MergeRequestEvent",
            created_at,
            actor_author,
            repo_dict,
            org_dict,
            payload_open,
            event_id("mr_open", mr.get("id") or mr.get("iid")),
        )
        emitted += 1

    emitted += iterate_items_until_stop_pages(
        url=url,
        params_base={"scope": "all", "state": "all", "order_by": "created_at", "sort": "desc"},
        get_created_at=lambda mr: mr.get("created_at"),
        on_item=on_mr_created,
    )

    def on_mr_updated(mr: dict, updated_at: str) -> None:
        nonlocal emitted
        merged_at = mr.get("merged_at")
        if merged_at and dt_ge_stop(merged_at):
            merger = (mr.get("merged_by") or mr.get("merge_user") or mr.get("author") or {})
            payload_merged = {
                "action": "closed",
                "pull_merged": 1,
                "merge_request": mr,
                "repository": repository_payload,
            }
            save_event(
                f_out,
                "MergeRequestEvent",
                merged_at,
                merger,
                repo_dict,
                org_dict,
                payload_merged,
                event_id("mr_merged", mr.get("id") or mr.get("iid")),
            )
            emitted += 1

    emitted += iterate_items_until_stop_pages(
        url=url,
        params_base={"scope": "all", "state": "merged", "order_by": "updated_at", "sort": "desc"},
        get_created_at=lambda mr: mr.get("updated_at"),
        on_item=on_mr_updated,
    )

    return emitted


def emit_mr_comment_events(f_out, base_url: str, repo_dict: dict, org_dict: dict, repository_payload: dict) -> int:
    mrs_url = f"{base_url}/merge_requests"

    mrs: List[dict] = []

    def collect_mr(mr: dict, created_at: str) -> None:
        mrs.append(mr)

    iterate_items_until_stop_pages(
        url=mrs_url,
        params_base={"scope": "all", "state": "all", "order_by": "created_at", "sort": "desc"},
        get_created_at=lambda mr: mr.get("created_at"),
        on_item=collect_mr,
    )

    emitted = 0
    for mr in mrs:
        iid = mr.get("iid")
        if iid is None:
            continue
        notes_url = f"{base_url}/merge_requests/{iid}/notes"

        def on_note(note: dict, created_at: str) -> None:
            nonlocal emitted
            actor = note.get("author") or {}
            payload = {"action": "created", "comment": note, "merge_request": mr, "repository": repository_payload}
            save_event(
                f_out,
                "MergeRequestCommentEvent",
                created_at,
                actor,
                repo_dict,
                org_dict,
                payload,
                event_id("mr_comment", note.get("id")),
            )
            emitted += 1

        emitted += iterate_items_until_stop_pages(
            url=notes_url,
            params_base={"order_by": "created_at", "sort": "desc"},
            get_created_at=lambda n: n.get("created_at"),
            on_item=on_note,
        )

    return emitted


def run_fetch(category_name: str, fn) -> None:
    print(f"  -> {category_name}...", end="", flush=True)
    count = fn()
    print(f" [Suma: {count}]")

def process_project(f_out, project: dict, group_dict: dict) -> None:
    base_url, repo_dict, repository_payload = build_project_context(project)

    run_fetch(
        "Issues opened (IssuesEvent)",
        lambda: emit_issue_opened_events(f_out, base_url, repo_dict, group_dict, repository_payload),
    )
    run_fetch(
        "Komentarze do issue (IssueCommentEvent via notes)",
        lambda: emit_issue_comment_events(f_out, base_url, repo_dict, group_dict, repository_payload),
    )
    run_fetch(
        "Merge Requests (opened + merged)",
        lambda: emit_merge_request_events(f_out, base_url, repo_dict, group_dict, repository_payload),
    )
    run_fetch(
        "Komentarze do MR (notes)",
        lambda: emit_mr_comment_events(f_out, base_url, repo_dict, group_dict, repository_payload),
    )

def main():
    print(f"[START] GitLab group={GROUP_PATH}, limit_projects={PROJECT_NUMBER_LIMIT}, DATE>={STOP_DATE.isoformat()}")

    group_meta = get_group_metadata(GROUP_PATH)

    projects = list_group_projects(GROUP_PATH)
    if not projects:
        print("[WARN] Nie znaleziono projektów.")
        return

    with gzip.open(OUTPUT_FILE, "wt", encoding="utf-8") as f_out:
        processed = 0
        for p in projects:
            if processed >= PROJECT_NUMBER_LIMIT:
                print(f"\n[LIMIT] Osiągnięto limit {PROJECT_NUMBER_LIMIT} projektów.")
                break

            full_name = p.get("path_with_namespace") or ""
            if full_name in EXCLUDED_PROJECTS:
                print(f"[SKIP] Pominięto projekt: {full_name}.")
                continue

            processed += 1
            print(f"\n[{processed}] {full_name}")
            process_project(f_out, p, group_meta)

    print(f"\nWynikowy plik: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
