CREATE DATABASE IF NOT EXISTS github_log;

USE github_log;

CREATE TABLE year2020
(
    event_time   DateTime,
    repo_id      UInt64,
    repo_name    String,
    actor_id     UInt64,
    actor_login  String,
    event_type   String
) ENGINE = MergeTree
PARTITION BY toYYYYMM(event_time)
ORDER BY (repo_id, actor_id, event_time);

CREATE TABLE agg_year2020
(
    repo_id             UInt64,
    repo_name           String,
    actor_id            UInt64,
    actor_login         String,
    hour                UInt8,
    week                UInt8,
    score               Float32,
    issue_comment       UInt32,
    open_issue          UInt32,
    open_pull           UInt32,
    pull_review_comment UInt32,
    merge_pull          UInt32
) ENGINE = MergeTree
ORDER BY (repo_id, actor_id, week, hour);

CREATE USER script_user IDENTIFIED WITH plaintext_password BY 'generic_password';
GRANT ALL ON github_log.* TO script_user WITH GRANT OPTION;
