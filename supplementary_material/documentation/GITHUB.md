### Badane typy aktywności
![img.png](img.png)

### kolumny w bazie zasilanej na podstawie uzyskanego pliku .json
opisane w pliku data_description/data_description.csv

### Użyte endpointy

* GET /orgs/{org}
    - pobiera metadane organizacji
    - get_org_dict()

* GET /orgs/{org}/repos 
    - pobiera listę repozytoriów wskazanej organizacji
    - get_all_org_repos()

* GET /repos/{owner}/{repo}
    - pobierane metadane każdego repozytorium
    - get_repo_meta(), build_repo_context()

* GET /repos/{owner}/{repo}/issues (IssuesEvent)
    - pobiera dane IssuesEvent opened, z filtrem na brak pull_request
    - paginacja
    - emit_issue_opened_events()

* GET /repos/{owner}/{repo}/issues/comments
    - pobiera dane IssueCommentEvent created
    - paginacja
    - emit_issue_comment_events()

* GET /repos/{owner}/{repo}/pulls 
    - dane do PullRequestEvent opened and merged (as closed+merged)
    - paginacja
    - emit_pull_request_events()
    - dodatkowo dla każdego pobranie szczegółów: GET /repos/{owner}/{repo}/pulls/{pull_number}
    - fetch_pr_full()

* GET /repos/{owner}/{repo}/pulls/{pull_number}/comments
    - dane dla PullRequestReviewCommentEvent created 
    - paginacja
    - emit_pr_review_comment_events()

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
