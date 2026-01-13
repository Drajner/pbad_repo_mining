

USE github_log;


INSERT INTO year2020 (event_time, repo_id, repo_name, actor_id, actor_login, event_type) VALUES
('2020-01-01 12:00:00', 20580498, 'kubernetes/kubernetes', 1, 'alice', 'PushEvent');

INSERT INTO agg_year2020 (repo_id, repo_name, actor_id, actor_login, hour, week, score,
                          issue_comment, open_issue, open_pull, pull_review_comment, merge_pull)
VALUES
(20580498, 'kubernetes/kubernetes', 1, 'alice', 12, 3, 1.0, 1, 0, 0, 0, 0);
