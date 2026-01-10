### Badane typy aktywności
![img.png](img.png)

### kolumny w bazie zasilanej na podstawie uzyskanego pliku .json
opisane w pliku data_description/data_description.csv

### Użyte endpointy

* GET /orgs/{org}/repos 
    - pobiera listę repozytoriów wskazanej organizacji

* GET /repos/{owner}/{repo}
    - pobierane metadane każdego repozytorium
    - JSON: repo_id, repo_name, repo_description, repo_size, repo_stargazers_count, repo_stargazers_count, repo_forks_count, repo_language, repo_has_issues, repo_has_projects, repo_has_downloads, repo_has_wiki, repo_has_pages, repo_license, repo_default_branch, repo_created_at, repo_updated_at, repo_pushed_at, org_id, org_login

* GET /repos/{owner}/{repo}/issues (IssuesEvent)
    - pobiera dane IssuesEvent opened, z filtrem na brak pull_request
    - paginacja
    - JSON: id, type, action, actor_id, actor_login, created_at, created_date, issue_id, issue_number, issue_title, issue_body, issue_author_id, issue_author_login, issue_author_type, issue_author_association, issue_assignee_id, issue_assignee_login, issue_created_at, issue_updated_at, issue_closed_at, issue_comments, issue_labels {name, color, default, description}, issue_assignees {id, login}

* GET /repos/{owner}/{repo}/issues/comments
    - pobiera dane IssueCommentEvent created
    - paginacja
    - dodatkowo dla każdego: GET /repos/{owner}/{repo}/issues/{issue_number} - dociągnięcie danych issue (issue_id, title, body)
    - JSON: id, type, action, actor_id, actor_login, created_at, created_date, issue_number, issue_id, issue_title, issue_body, issue_comment_id, issue_comment_body, issue_comment_created_at, issue_comment_updated_at, issue_comment_author_association, issue_comment_author_id, issue_comment_author_login, issue_comment_author_type

* GET /repos/{owner}/{repo}/pulls 
    - dane do PullRequestEvent opened and merged (as closed+merged)
    - paginacja
    - dodatkowo dla każdego GET /repos/{owner}/{repo}/pulls/{pull_number}: zebranie szczegółów PR, statystyk i merged fields
    - JSON dla opened: id, type, action, actor_id, actor_login, created_at, created_date, issue_number, pull_commits, pull_additions, pull_deletions, pull_changed_files, pull_review_comments, pull_requested_reviewer_id, pull_requested_reviewer_login, pull_requested_reviewer_type
    - JSON dla merged: id, type, action, actor_id, actor_login, created_at, created_date, issue_number, pull_merged, pull_merged_at, pull_merged_commit_sha, pull_merged_by_id, pull_merged_by_login, pull_merged_by_type

* GET /repos/{owner}/{repo}/pulls/{pull_number}/comments
    - dane dla PullRequestReviewCommentEvent created 
    - wykorzystują pr_number z wczesniejszego enpointu
    - paginacja
    - JSON: id, type, action, actor_id, actor_login, created_at, created_date, issue_number, pull_review_id, pull_review_comment_id, pull_review_comment_path, pull_review_comment_position, pull_review_comment_author_id, pull_review_comment_author_login, pull_review_comment_author_type, pull_review_comment_author_association, pull_review_comment_body, pull_review_comment_created_at, pull_review_comment_updated_at    

### Rate limity i http statuses
    - 200 OK
    - 401 Unauthorized
    - 403 Forbidden
    - 404 Not Found
    - 422 Unprocessable Entity: oznacza limit paginacji dla dużych datasetów
    - 403 i 429 może oznaczać rate limit
    - w celu monitorowania rate limitów/retry należy odczytywać nagłówki:
        X-RateLimit-Limit
        X-RateLimit-Remaining
        X-RateLimit-Reset
        Retry-After
