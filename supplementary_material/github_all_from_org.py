import gzip
import json
import time
from datetime import datetime
import requests

# --- CONFIGURATION ---
GITHUB_TOKEN = "asd"
ORG_NAME = "WiseLibs" 
OUTPUT_FILE = "org_historical_data.json.gz"

# Fetch data back until this date
STOP_DATE = datetime(2024, 1, 1) # Set this to your desired start date

HEADERS = {
    "Authorization": f"token {GITHUB_TOKEN}",
    "Accept": "application/vnd.github.v3+json",
}

def get_org_repos(org):
    repos = []
    page = 1
    print(f"--- Fetching list of repositories for org: {org} ---")
    while True:
        url = f"https://api.github.com/orgs/{org}/repos"
        params = {"per_page": 100, "page": page}
        try:
            resp = requests.get(url, headers=HEADERS, params=params)
            resp.raise_for_status()
            data = resp.json()
            if not data: break
            for r in data:
                repos.append(r['name'])
            page += 1
        except Exception as e:
            print(f"Error fetching repo list: {e}")
            break
    return repos

def save_event(file_obj, event_type, created_at, actor, payload, repo_data):
    synthetic_event = {
        "id": str(payload.get("id", int(time.time() * 1000))),
        "type": event_type,
        "actor": actor,
        "repo": repo_data,
        "payload": payload,
        "created_at": created_at,
        "org": {"login": ORG_NAME}, 
    }
    json.dump(synthetic_event, file_obj, ensure_ascii=False)
    file_obj.write("\n")

def fetch_commits(f_out, owner, repo):
    # FIXED URL: Removed the extra 'api.'
    url = f"https://api.github.com/repos/{owner}/{repo}/commits"
    page = 1
    while True:
        params = {"per_page": 100, "page": page}
        resp = requests.get(url, headers=HEADERS, params=params)
        if resp.status_code != 200: break
        
        commits = resp.json()
        if not commits: break
        
        for c in commits:
            date_str = c["commit"]["author"]["date"]
            dt_obj = datetime.fromisoformat(date_str.replace("Z", "+00:00")).replace(tzinfo=None)
            if dt_obj < STOP_DATE: return
            
            save_event(f_out, "PushEvent", date_str, c.get("author") or {"login": "unknown"}, 
                       {"sha": c["sha"]}, {"name": f"{owner}/{repo}"})
        page += 1

def fetch_pull_requests(f_out, owner, repo):
    url = f"https://api.github.com/repos/{owner}/{repo}/pulls"
    page = 1
    while True:
        params = {"per_page": 100, "page": page, "state": "all", "sort": "created", "direction": "desc"}
        resp = requests.get(url, headers=HEADERS, params=params)
        if resp.status_code != 200: break
        prs = resp.json()
        if not prs: break
        for pr in prs:
            date_str = pr["created_at"]
            dt_obj = datetime.fromisoformat(date_str.replace("Z", "+00:00")).replace(tzinfo=None)
            if dt_obj < STOP_DATE: return
            save_event(f_out, "PullRequestEvent", date_str, pr["user"], {"action": "opened", "pull_request": pr}, {"name": f"{owner}/{repo}"})
        page += 1

def fetch_issues(f_out, owner, repo):
    url = f"https://api.github.com/repos/{owner}/{repo}/issues"
    page = 1
    while True:
        params = {"per_page": 100, "page": page, "state": "all", "sort": "created", "direction": "desc"}
        resp = requests.get(url, headers=HEADERS, params=params)
        if resp.status_code != 200: break
        issues = resp.json()
        if not issues: break
        for issue in issues:
            if "pull_request" in issue: continue
            date_str = issue["created_at"]
            dt_obj = datetime.fromisoformat(date_str.replace("Z", "+00:00")).replace(tzinfo=None)
            if dt_obj < STOP_DATE: return
            save_event(f_out, "IssuesEvent", date_str, issue["user"], {"action": "opened", "issue": issue}, {"name": f"{owner}/{repo}"})
        page += 1

def main():
    if GITHUB_TOKEN == "your_token_here":
        print("Please update your GITHUB_TOKEN!")
        return

    all_repos = get_org_repos(ORG_NAME)
    print(f"Found {len(all_repos)} repositories in {ORG_NAME}.")

    with gzip.open(OUTPUT_FILE, "wt", encoding="utf-8") as f:
        for repo_name in all_repos:
            print(f"\n>>> PROCESSING REPO: {repo_name} <<<")
            try:
                # Wrap each repo in a try/except so one failure doesn't stop the whole script
                fetch_commits(f, ORG_NAME, repo_name)
                fetch_pull_requests(f, ORG_NAME, repo_name)
                fetch_issues(f, ORG_NAME, repo_name)
            except Exception as e:
                print(f"Error processing {repo_name}: {e}")

    print(f"\nFinished. Data saved to {OUTPUT_FILE}")

if __name__ == "__main__":
    main()