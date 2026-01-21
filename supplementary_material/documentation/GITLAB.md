# Skrypt gitlab.py

Skrypt pobiera dane aktywności z Gitlab.com dla projektów należących do wskazanej grupy. 
GitLab nie udostępnia API do pobierania eventów, więc podobnie jak w skrypcie github pobieramy kolejne issues, merge requests, comments etc. i konwertujemy je do odpowiedniego do dalszej analizy formatu, zbliżonego do GitHub events.

## Konfiguracja
* GITLAB_TOKEN - pobierany jako zmienna środowiskowa
* GROUP_PATH - ścieżka grupy
* PROJECT_NUMBER_LIMIT - ile projektów maksymalnie przetworzyć
* EXCLUDED_PROJECTS - lista do pominięcia
* PER_PAGE - rozmiar strony w paginacji

## Format pliku wynikowego 
`
{
  "id": "prefix:123", 
  "type": "IssuesEvent | IssueCommentEvent | MergeRequestEvent | MergeRequestCommentEvent", 
  "created_at": "2025-01-14T...", 
  "actor": { ... }, 
  "repo": { "id": ..., "name": ... }, 
  "org": { "id": ..., "login": ... }, 
  "payload": { ... }
}
`

## Anonimizacja (SHA-256 z solą)
* hashujemy:
  - użytkownikó (id i username)
  - dane repo (id i name)
  - dane organizacji (id i login)
* boty zostawiamy jawnie

## Redagowanie tekstu 
* pola tekstowe, które mogłyby zawierać wrażliwe dane (title, body, description) zamieniamy na stały placeholder "xyz"

## Używane endpointy
1. GET /groups/:group_path 
pobranie metadanych grupy
2. GET /groups/:group_path/projects 
pobranie listy projektów
3. GET /projects/:project_id/issues 
pobranie listy issues
5. GET /projects/:project_id/issues/:iid/notes 
szczegóły poszczególnych issues
6. GET /projects/:project_id/merge_requests
lista merge requests
7. GET /projects/:project_id/merge_requests/:iid/notes
pobranie szczegółów merge requestow

# Typy eventow
* issue_open
* issue_comment
* mr_open
* mr_merged
* mr_comment
