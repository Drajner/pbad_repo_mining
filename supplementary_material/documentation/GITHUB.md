# Badane typy aktywności
![img.png](img.png)

# GET /orgs/{org}/repos 
Lista repozytoriów w organizacji

# GET /repos/{owner}/{repo}
Metadane repo

# GET /repos/{owner}/{repo}/issues
issue (do IssuesEvent opened, z filtrem na brak pull_request)

# GET /repos/{owner}/{repo}/issues/comments
komentarze do issue/PR (do IssueCommentEvent created)

# GET /repos/{owner}/{repo}/issues/{issue_number}
dociągnięcie danych issue do komentarza (enrichment)

# GET /repos/{owner}/{repo}/pulls 
PR (do PullRequestEvent opened, lista PR)

# GET /repos/{owner}/{repo}/pulls/{pull_number}
szczegóły PR (statystyki + merged fields)

# GET /repos/{owner}/{repo}/pulls/{pull_number}/comments
komentarze review w PR (do PullRequestReviewCommentEvent created)