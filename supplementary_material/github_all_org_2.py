import gzip
import json
import time
from datetime import datetime
import requests
import sys

# --- CONFIGURATION ---
GITHUB_TOKEN = "asd"
ORG_NAME = "kubernetes" 
OUTPUT_FILE = "kubernetes_to_2024.json.gz"
STOP_DATE = datetime(2024, 1, 1) 

HEADERS = {
    "Authorization": f"token {GITHUB_TOKEN}",
    "Accept": "application/vnd.github.v3+json",
}

def is_bot(user_login):
    """Identyfikacja botów zgodnie z metodologią """
    if not user_login: return False
    return user_login.lower().endswith("[bot]") or user_login.lower().endswith("bot")

def handle_rate_limit(response):
    if response.status_code == 403 and 'X-RateLimit-Remaining' in response.headers:
        if int(response.headers['X-RateLimit-Remaining']) == 0:
            reset_time = int(response.headers['X-RateLimit-Reset'])
            sleep_duration = max(reset_time - int(time.time()), 0) + 5
            print(f"\n[LIMIT] Czekanie do {datetime.fromtimestamp(reset_time).strftime('%H:%M:%S')}...")
            time.sleep(sleep_duration)
            return True
    return False

def make_request(url, params=None):
    for attempt in range(3):
        try:
            resp = requests.get(url, headers=HEADERS, params=params, timeout=30)
            if handle_rate_limit(resp): return make_request(url, params)
            resp.raise_for_status()
            return resp
        except Exception as e:
            print(f"  [BŁĄD] {e}. Próba {attempt+1}/3...")
            time.sleep(2 * (attempt + 1))
    return None

def save_event(file_obj, event_type, created_at, actor_dict, payload_dict, repo_name):
    """Zapisuje dane w formacie płaskim, gdzie obiekty są ciągami String (JSON)"""
    
    # Budowa struktury org zgodnie z wymaganiami ClickHouse
    org_dict = {"id": 0, "login": ORG_NAME, "url": f"https://api.github.com/orgs/{ORG_NAME}"}
    
    # Budowa struktury repo
    repo_dict = {"id": 0, "name": f"{ORG_NAME}/{repo_name}", "url": f"https://api.github.com/repos/{ORG_NAME}/{repo_name}"}

    # Tworzenie rekordu JSONEachRow
    # Wszystkie pola obiektowe są zamieniane na String przez json.dumps
    event = {
        "id": str(int(time.time() * 1000000)),
        "type": event_type,
        "actor": json.dumps(actor_dict, ensure_ascii=False),
        "repo": json.dumps(repo_dict, ensure_ascii=False),
        "org": json.dumps(org_dict, ensure_ascii=False),
        "payload": json.dumps(payload_dict, ensure_ascii=False),
        "created_at": created_at
    }
    
    json.dump(event, file_obj, ensure_ascii=False)
    file_obj.write("\n")

def fetch_data(url, f_out, repo_name, category_name, process_callback):
    print(f"  -> {category_name}...", end="", flush=True)
    page, count = 1, 0
    while True:
        resp = make_request(url, {"per_page": 100, "page": page, "sort": "created", "direction": "desc"})
        if not resp: break
        items = resp.json()
        if not items: break
        
        for item in items:
            date_str = item.get("created_at")
            if not date_str: continue
            dt_obj = datetime.fromisoformat(date_str.replace("Z", "+00:00")).replace(tzinfo=None)
            if dt_obj < STOP_DATE:
                print(f" [Suma: {count}]")
                return
            process_callback(item, date_str)
            count += 1
        page += 1
    print(f" [Suma: {count}]")

def process_repo(f_out, repo_name):
    base_url = f"https://api.github.com/repos/{ORG_NAME}/{repo_name}"
    
    # b1: Komentarze (IssueCommentEvent) [cite: 141]
    fetch_data(f"{base_url}/issues/comments", f_out, repo_name, "Komentarze",
               lambda item, ds: save_event(f_out, "IssueCommentEvent", ds, item["user"], {"action": "created", "comment": item}, repo_name))

    # b2: Zgłoszenia (IssuesEvent) [cite: 141]
    fetch_data(f"{base_url}/issues", f_out, repo_name, "Issues",
               lambda item, ds: save_event(f_out, "IssuesEvent", ds, item["user"], {"action": "opened", "issue": item}, repo_name) if "pull_request" not in item else None)

    # b3 & b5: Pull Requests (PullRequestEvent) [cite: 141]
    def handle_pr(item, ds):
        save_event(f_out, "PullRequestEvent", ds, item["user"], {"action": "opened", "pull_request": item}, repo_name)
        if item.get("merged_at"):
            save_event(f_out, "PullRequestEvent", item["merged_at"], item["user"], {"action": "closed", "pull_merged": True, "pull_request": item}, repo_name)

    fetch_data(f"{base_url}/pulls", f_out, repo_name, "Pull Requests", handle_pr)

    # b4: Recenzje (PullRequestReviewCommentEvent) [cite: 141]
    fetch_data(f"{base_url}/pulls/comments", f_out, repo_name, "Recenzje",
               lambda item, ds: save_event(f_out, "PullRequestReviewCommentEvent", ds, item["user"], {"action": "created", "comment": item}, repo_name))

def main():
    print(f"Rozpoczęto pobieranie dla {ORG_NAME} (Format ClickHouse JSONEachRow)")
    repos_resp = make_request(f"https://api.github.com/orgs/{ORG_NAME}/repos", {"per_page": 100})
    if not repos_resp: return
    repos = [r['name'] for r in repos_resp.json()]

    with gzip.open(OUTPUT_FILE, "wt", encoding="utf-8") as f:
        for idx, repo in enumerate(repos, 1):
            print(f"[{idx}/{len(repos)}] Repo: {repo}")
            process_repo(f, repo)
    print(f"Sukces. Plik: {OUTPUT_FILE}")

if __name__ == "__main__":
    main()