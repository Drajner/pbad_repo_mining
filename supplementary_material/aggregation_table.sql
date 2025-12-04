-- ---------------------------------------------------------
-- Aggregation table + materialized view for gharchive
-- Based on:
--   - base table: gharchive.github_events
--   - agg_description.csv (repo_id, repo_name, actor_id, actor_login, week, hour, metrics...)
-- ---------------------------------------------------------

CREATE DATABASE IF NOT EXISTS github_log;

USE github_log;

-- ---------------------------------------------------------
-- 1. Aggregation table (names & types from agg_description.csv)
-- ---------------------------------------------------------

CREATE TABLE IF NOT EXISTS agg_year2020
(
    repo_id UInt64,
    repo_name String,
    actor_id UInt64,
    actor_login String,
    week UInt8,                  -- day of week (1–7)
    hour UInt8,                  -- hour of day (0–23)

    issue_comment UInt64,        -- # IssueCommentEvent
    open_issue UInt64,           -- # IssuesEvent with action='opened'
    open_pull UInt64,            -- # PullRequestEvent with action='opened'
    pull_review_comment UInt64,  -- # PullRequestReviewCommentEvent
    merge_pull UInt64,           -- # merged pull requests
    score UInt64                 -- 1*issue_comment + 2*open_issue + 3*open_pull + 4*pull_review_comment + 5*merge_pull
)
ENGINE = SummingMergeTree()
PARTITION BY week
ORDER BY (repo_id, actor_id, week, hour);

-- ---------------------------------------------------------
-- 2. Materialized view: aggregate from github_events
--
-- Assumptions (standard GHArchive semantics):
--   type = 'IssueCommentEvent'                  → issue_comment
--   type = 'IssuesEvent'  AND action='opened'   → open_issue
--   type = 'PullRequestEvent' AND action='opened' → open_pull
--   type = 'PullRequestReviewCommentEvent'      → pull_review_comment
--   type = 'PullRequestEvent' AND pull_merged = 1 → merge_pull
--
-- Time bucketing:
--   week = toDayOfWeek(created_at)
--   hour = toHour(created_at)
-- ---------------------------------------------------------

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_agg_year2020
TO github_log.agg_year2020
AS
SELECT
    repo_id,
    repo_name,
    actor_id,
    actor_login,
    week,
    hour,
    issue_comment,
    open_issue,
    open_pull,
    pull_review_comment,
    merge_pull,
    (issue_comment
     + 2 * open_issue
     + 3 * open_pull
     + 4 * pull_review_comment
     + 5 * merge_pull) AS score
FROM
(
    SELECT
        repo_id,
        repo_name,
        actor_id,
        actor_login,
        toDayOfWeek(created_at) AS week,
        toHour(created_at)      AS hour,

        countIf(type = 'IssueCommentEvent') AS issue_comment,
        countIf(type = 'IssuesEvent' AND action = 'opened') AS open_issue,
        countIf(type = 'PullRequestEvent' AND action = 'opened') AS open_pull,
        countIf(type = 'PullRequestReviewCommentEvent') AS pull_review_comment,
        countIf(type = 'PullRequestEvent' AND pull_merged = 1) AS merge_pull
    FROM github_log.year2020
    GROUP BY
        repo_id,
        repo_name,
        actor_id,
        actor_login,
        week,
        hour
);