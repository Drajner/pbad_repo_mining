import gzip
import json
import time
from datetime import datetime

import requests

# --- KONFIGURACJA ---
GITHUB_TOKEN = "asd"
REPO_OWNER = "kubernetes"
REPO_NAME = "kubernetes"
OUTPUT_FILE = "github_historical_data.json.gz"

# Pobieraj dane cofając się w przeszłość AŻ do tej daty (np. początek 2020 roku)
STOP_DATE = datetime(2025, 6, 1)

HEADERS = {
    "Authorization": f"token {GITHUB_TOKEN}",
    "Accept": "application/vnd.github.v3+json",
}


def save_event(file_obj, event_type, created_at, actor, payload, repo_data):
    """
    Tworzy syntetyczne zdarzenie pasujące do struktury Twojego pliku SQL.
    """
    synthetic_event = {
        "id": str(payload.get("id", int(time.time() * 1000))),  # Fake ID jeśli brak
        "type": event_type,
        "actor": actor,
        "repo": repo_data,
        "payload": payload,
        "created_at": created_at,
        "org": repo_data.get("owner", {}),  # Uproszczenie
    }
    json.dump(synthetic_event, file_obj, ensure_ascii=False)
    file_obj.write("\n")


def fetch_commits(f_out):
    """
    Pobiera commity (Code Contribution) - ważne dla stref czasowych[cite: 94].
    """
    print(f"--- Pobieranie COMMITÓW ---")
    url = f"https://api.github.com/repos/{REPO_OWNER}/{REPO_NAME}/commits"
    page = 1

    while True:
        print(f"Commits Page {page}...")
        params = {
            "per_page": 100,
            "page": page,
        }  # Commity są domyślnie sortowane od najnowszych

        try:
            resp = requests.get(url, headers=HEADERS, params=params)
            if resp.status_code != 200:
                break

            commits = resp.json()
            if not commits:
                break

            for c in commits:
                date_str = c["commit"]["author"]["date"]
                dt_obj = datetime.fromisoformat(
                    date_str.replace("Z", "+00:00")
                ).replace(tzinfo=None)

                # WARUNEK STOPU
                if dt_obj < STOP_DATE:
                    print(f"Osiągnięto datę graniczną dla commitów: {date_str}")
                    return

                # Budowanie struktury "PushEvent" (uproszczonej) dla SQL
                actor_data = c.get("author") or {
                    "id": 0,
                    "login": c["commit"]["author"]["name"],
                }

                payload = {
                    "push_id": c["sha"],
                    "size": 1,
                    "distinct_size": 1,
                    "ref": "refs/heads/master",  # Założenie
                    "head": c["sha"],
                    "commits": [c["commit"]],
                }

                save_event(
                    f_out,
                    "PushEvent",
                    date_str,
                    actor_data,
                    payload,
                    {"id": 0, "name": f"{REPO_OWNER}/{REPO_NAME}"},
                )

            page += 1
        except Exception as e:
            print(f"Error commits: {e}")
            break


def fetch_pull_requests(f_out):
    """
    Pobiera PRy (Collaboration) - kluczowe dla Activity Model.
    """
    print(f"--- Pobieranie PULL REQUESTS ---")
    url = f"https://api.github.com/repos/{REPO_OWNER}/{REPO_NAME}/pulls"
    page = 1

    while True:
        print(f"PRs Page {page}...")
        # state='all' pobiera też zamknięte/zmergowane
        params = {
            "per_page": 100,
            "page": page,
            "state": "all",
            "sort": "created",
            "direction": "desc",
        }

        try:
            resp = requests.get(url, headers=HEADERS, params=params)
            if resp.status_code != 200:
                break

            prs = resp.json()
            if not prs:
                break

            for pr in prs:
                date_str = pr["created_at"]
                dt_obj = datetime.fromisoformat(
                    date_str.replace("Z", "+00:00")
                ).replace(tzinfo=None)

                if dt_obj < STOP_DATE:
                    print(f"Osiągnięto datę graniczną dla PR: {date_str}")
                    return

                # Mapowanie na PullRequestEvent
                # WAŻNE: W Activity Model interesuje nas "action": "opened" lub "closed" (merge)
                # Tutaj symulujemy zdarzenie "opened" na podstawie daty utworzenia

                payload = {
                    "action": "opened",
                    "pull_request": pr,
                    "number": pr["number"],
                }

                save_event(
                    f_out,
                    "PullRequestEvent",
                    date_str,
                    pr["user"],
                    payload,
                    {"id": 0, "name": f"{REPO_OWNER}/{REPO_NAME}"},
                )

                # Jeśli PR jest zmergowany, można by dodać drugie zdarzenie "merged",
                # ale wymagałoby to sprawdzenia daty 'merged_at'.

            page += 1
        except Exception as e:
            print(f"Error PRs: {e}")
            break


def fetch_issues(f_out):
    """
    Pobiera Issues (bez PRów) - również część Activity Model.
    """
    print(f"--- Pobieranie ISSUES ---")
    url = f"https://api.github.com/repos/{REPO_OWNER}/{REPO_NAME}/issues"
    page = 1

    while True:
        print(f"Issues Page {page}...")
        params = {
            "per_page": 100,
            "page": page,
            "state": "all",
            "sort": "created",
            "direction": "desc",
        }

        try:
            resp = requests.get(url, headers=HEADERS, params=params)
            if resp.status_code != 200:
                break

            issues = resp.json()
            if not issues:
                break

            for issue in issues:
                # API /issues zwraca też PRy, musimy je pominąć
                if "pull_request" in issue:
                    continue

                date_str = issue["created_at"]
                dt_obj = datetime.fromisoformat(
                    date_str.replace("Z", "+00:00")
                ).replace(tzinfo=None)

                if dt_obj < STOP_DATE:
                    print(f"Osiągnięto datę graniczną dla Issues: {date_str}")
                    return

                payload = {"action": "opened", "issue": issue}

                save_event(
                    f_out,
                    "IssuesEvent",
                    date_str,
                    issue["user"],
                    payload,
                    {"id": 0, "name": f"{REPO_OWNER}/{REPO_NAME}"},
                )

            page += 1
        except Exception as e:
            print(f"Error Issues: {e}")
            break


def main():
    if GITHUB_TOKEN == "TWOJ_TOKEN_GITHUB":
        print("UZUPEŁNIJ TOKEN!")
        return

    # Otwieramy jeden wspólny plik .gz na wszystkie dane
    with gzip.open(OUTPUT_FILE, "wt", encoding="utf-8") as f:
        fetch_commits(f)
        fetch_pull_requests(f)
        fetch_issues(f)

    print(f"\nZakończono. Wszystkie dane zapisane w {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
