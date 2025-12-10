USE github_log;

CREATE TABLE IF NOT EXISTS agg_year2020
(
    actor_id            UInt64,
    actor_login         String,
    repo_id             UInt64,
    repo_name           String,
    hour                UInt8,
    week                UInt8,
    issue_comment       UInt32,
    open_issue          UInt32,
    open_pull           UInt32,
    pull_review_comment UInt32,
    merge_pull          UInt32,
    score               Float64
)
ENGINE = MergeTree
ORDER BY (repo_id, actor_id, week, hour);

INSERT INTO agg_year2020
SELECT
    actor_id,
    actor_login,
    repo_id,
    repo_name,
    toHour(created_at)     AS hour,
    toDayOfWeek(created_at) AS week,

    countIf(type = 'IssueCommentEvent')                                                AS issue_comment,
    countIf(type = 'IssuesEvent'       AND action = 'opened')                          AS open_issue,
    countIf(type = 'PullRequestEvent'  AND action = 'opened')                          AS open_pull,
    countIf(type = 'PullRequestReviewCommentEvent')                                    AS pull_review_comment,
    countIf(type = 'PullRequestEvent'  AND action = 'closed')                          AS merge_pull,

    issue_comment * 1
      + open_issue * 2
      + open_pull * 3
      + merge_pull * 4
      + pull_review_comment * 2                                                       AS score
FROM year2020
GROUP BY
    actor_id,
    actor_login,
    repo_id,
    repo_name,
    hour,
    week;
