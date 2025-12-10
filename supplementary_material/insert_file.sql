USE github_log;

INSERT INTO year2020
(
    -- CORE EVENT FIELDS
    id,
    type,
    action,
    actor_id,
    actor_login,
    repo_id,
    repo_name,
    org_id,
    org_login,
    created_at,
    created_date,

    -- REPO (if you have only repo_id / repo_name, drop the rest)
    -- repo_url,
    -- repo_description,
    -- repo_language,
    -- repo_stargazers_count,
    -- repo_forks_count,
    -- repo_open_issues_count,
    -- repo_size,
    -- repo_has_issues,
    -- repo_has_projects,
    -- repo_has_downloads,
    -- repo_has_wiki,
    -- repo_has_pages,
    -- repo_default_branch,
    -- repo_created_at,
    -- repo_pushed_at,
    -- repo_updated_at,

    -------------------------------------------------------------
    -- ISSUE FIELDS (IssuesEvent, PullRequestEvent, etc.)
    -------------------------------------------------------------
    issue_id,
    issue_number,
    issue_title,
    issue_body,
    -- issue_state,
    -- issue_locked,
    -- issue_comments,
    -- issue_created_at,
    -- issue_updated_at,
    -- issue_closed_at,

    -- issue_author_id,
    -- issue_author_login,
    -- issue_author_type,
    -- issue_author_association,

    -- issue_assignee_id,
    -- issue_assignee_login,

    -------------------------------------------------------------
    -- ISSUE COMMENT (IssueCommentEvent)
    -------------------------------------------------------------
    issue_comment_id,
    issue_comment_body,
    -- issue_comment_created_at,
    -- issue_comment_updated_at,
    issue_comment_author_association,
    issue_comment_author_id,
    issue_comment_author_login,
    issue_comment_author_type,

    -------------------------------------------------------------
    -- PULL REQUEST FIELDS (PullRequestEvent & others where payload.pull_request exists)
    -------------------------------------------------------------
    -- pull_id,
    -- pull_number,
    -- pull_state,
    -- pull_locked,
    -- pull_title,
    -- pull_body,
    -- pull_created_at,
    -- pull_updated_at,
    -- pull_closed_at,

    -- pull_merged,
    -- pull_merged_at,
    -- pull_merge_commit_sha,
    -- pull_merged_by_id,
    -- pull_merged_by_login,
    -- pull_merged_by_type,

    -- pull_commits,
    -- pull_additions,
    -- pull_deletions,
    -- pull_changed_files,
    -- pull_comments,
    -- pull_review_comments,

    -- pull_base_ref,
    -- pull_head_ref,
    -- pull_base_sha,
    -- pull_head_sha,

    -- pull_author_association,
    -- pull_draft,

    -- pull_requested_reviewer_id,
    -- pull_requested_reviewer_login,
    -- pull_requested_reviewer_type,

    -------------------------------------------------------------
    -- PULL REQUEST REVIEW COMMENT (PullRequestReviewCommentEvent)
    -------------------------------------------------------------
    -- pull_review_comment_id,
    -- pull_review_comment_body,
    -- pull_review_comment_path,
    -- pull_review_comment_position,
    -- pull_review_comment_commit_id,
    -- pull_review_comment_original_commit_id,
    -- pull_review_comment_created_at,
    -- pull_review_comment_updated_at,
    -- pull_review_comment_author_association,
    -- pull_review_comment_author_id,
    -- pull_review_comment_author_login,
    -- pull_review_comment_author_type,

    -------------------------------------------------------------
    -- COMMIT COMMENT (CommitCommentEvent)
    -------------------------------------------------------------
    commit_comment_id,
    commit_comment_body,
    commit_comment_path,
    commit_comment_position,
    commit_comment_line,
    -- commit_comment_created_at,
    -- commit_comment_updated_at,
    commit_comment_author_association,
    commit_comment_author_id,
    commit_comment_author_login,
    commit_comment_author_type,

    -------------------------------------------------------------
    -- PUSH (PushEvent)
    -------------------------------------------------------------
    push_id,
    push_size,
    push_distinct_size,
    push_ref,
    push_head,
    push_before,

    -------------------------------------------------------------
    -- FORK (ForkEvent)
    -------------------------------------------------------------
    fork_forkee_id,
    fork_forkee_full_name,
    fork_forkee_owner_id,
    fork_forkee_owner_login,
    fork_forkee_owner_type,

    -------------------------------------------------------------
    -- CREATE / DELETE (CreateEvent / DeleteEvent)
    -------------------------------------------------------------
    create_ref,
    create_ref_type,
    create_master_branch,
    create_description,
    create_pusher_type,

    delete_ref,
    delete_ref_type,
    delete_pusher_type,

    -------------------------------------------------------------
    -- MEMBER (MemberEvent)
    -------------------------------------------------------------
    member_id,
    member_login,
    member_type,

    -------------------------------------------------------------
    -- RELEASE (ReleaseEvent)
    -------------------------------------------------------------
    release_id,
    release_tag_name,
    release_target_commitish,
    release_name,
    release_draft,
    release_author_id,
    release_author_login,
    release_author_type,
    release_prerelease,
    -- release_created_at,
    -- release_published_at,
    release_body
)
SELECT
    ---------------------------------------------------------
    -- CORE EVENT FIELDS (always present)
    ---------------------------------------------------------
    id,
    type,

    JSONExtractString(payload, 'action')                                         AS action,

    JSONExtractUInt(actor, 'id')                                                 AS actor_id,
    JSONExtractString(actor, 'login')                                            AS actor_login,

    JSONExtractUInt(repo, 'id')                                                  AS repo_id,
    JSONExtractString(repo, 'name')                                              AS repo_name,

    JSONExtractUInt(org, 'id')                                                   AS org_id,
    JSONExtractString(org, 'login')                                              AS org_login,

    parseDateTimeBestEffort(created_at)                                          AS created_at,
    toDate(created_at)                                                           AS created_date,

    ---------------------------------------------------------
    -- REPOSITORY FIELDS (best effort, many events don’t embed full repo)
    -- Adjust to your table: drop these columns if you don’t have them,
    -- or keep them as NULL/empty for events that don’t provide data.
    ---------------------------------------------------------
    -- JSONExtractString(payload, 'repository.url')                                 AS repo_url,
    -- JSONExtractString(payload, 'repository.description')                         AS repo_description,
    -- JSONExtractString(payload, 'repository.language')                            AS repo_language,
    -- JSONExtractUInt  (payload, 'repository.stargazers_count')                    AS repo_stargazers_count,
    -- JSONExtractUInt  (payload, 'repository.forks_count')                         AS repo_forks_count,
    -- JSONExtractUInt  (payload, 'repository.open_issues_count')                   AS repo_open_issues_count,
    -- JSONExtractUInt  (payload, 'repository.size')                                AS repo_size,
    -- JSONExtractBool  (payload, 'repository.has_issues')                          AS repo_has_issues,
    -- JSONExtractBool  (payload, 'repository.has_projects')                        AS repo_has_projects,
    -- JSONExtractBool  (payload, 'repository.has_downloads')                       AS repo_has_downloads,
    -- JSONExtractBool  (payload, 'repository.has_wiki')                            AS repo_has_wiki,
    -- JSONExtractBool  (payload, 'repository.has_pages')                           AS repo_has_pages,
    -- JSONExtractString(payload, 'repository.default_branch')                      AS repo_default_branch,
    -- JSONExtractDateTime(payload, 'repository.created_at')                        AS repo_created_at,
    -- JSONExtractDateTime(payload, 'repository.pushed_at')                         AS repo_pushed_at,
    -- JSONExtractDateTime(payload, 'repository.updated_at')                        AS repo_updated_at,

    ---------------------------------------------------------
    -- ISSUE FIELDS (IssuesEvent + PR-as-issue)
    ---------------------------------------------------------
    JSONExtractUInt   (payload, 'issue.id')                                      AS issue_id,
    JSONExtractUInt   (payload, 'issue.number')                                  AS issue_number,
    JSONExtractString (payload, 'issue.title')                                   AS issue_title,
    JSONExtractString (payload, 'issue.body')                                    AS issue_body,
    -- JSONExtractString (payload, 'issue.state')                                   AS issue_state,
    -- JSONExtractBool   (payload, 'issue.locked')                                  AS issue_locked,
    -- JSONExtractUInt   (payload, 'issue.comments')                                AS issue_comments,
    -- JSONExtractDateTime(payload, 'issue.created_at')                             AS issue_created_at,
    -- JSONExtractDateTime(payload, 'issue.updated_at')                             AS issue_updated_at,
    -- JSONExtractDateTime(payload, 'issue.closed_at')                              AS issue_closed_at,

    -- JSONExtractUInt   (payload, 'issue.user.id')                                 AS issue_author_id,
    -- JSONExtractString (payload, 'issue.user.login')                              AS issue_author_login,
    -- JSONExtractString (payload, 'issue.user.type')                               AS issue_author_type,
    -- JSONExtractString (payload, 'issue.author_association')                      AS issue_author_association,

    -- JSONExtractUInt   (payload, 'issue.assignee.id')                             AS issue_assignee_id,
    -- JSONExtractString (payload, 'issue.assignee.login')                          AS issue_assignee_login,

    ---------------------------------------------------------
    -- ISSUE COMMENT (IssueCommentEvent)
    ---------------------------------------------------------
    JSONExtractUInt   (payload, 'comment.id')                                    AS issue_comment_id,
    JSONExtractString (payload, 'comment.body')                                  AS issue_comment_body,
    -- JSONExtractDateTime(payload, 'comment.created_at')                           AS issue_comment_created_at,
    -- JSONExtractDateTime(payload, 'comment.updated_at')                           AS issue_comment_updated_at,
    JSONExtractString (payload, 'comment.author_association')                    AS issue_comment_author_association,
    JSONExtractUInt   (payload, 'comment.user.id')                               AS issue_comment_author_id,
    JSONExtractString (payload, 'comment.user.login')                            AS issue_comment_author_login,
    JSONExtractString (payload, 'comment.user.type')                             AS issue_comment_author_type,

    ---------------------------------------------------------
    -- PULL REQUEST (payload.pull_request.*)
    ---------------------------------------------------------
    -- JSONExtractUInt   (payload, 'pull_request.id')                               AS pull_id,
    -- JSONExtractUInt   (payload, 'pull_request.number')                           AS pull_number,
    -- JSONExtractString (payload, 'pull_request.state')                            AS pull_state,
    -- JSONExtractBool   (payload, 'pull_request.locked')                           AS pull_locked,
    -- JSONExtractString (payload, 'pull_request.title')                            AS pull_title,
    -- JSONExtractString (payload, 'pull_request.body')                             AS pull_body,
    -- JSONExtractDateTime(payload, 'pull_request.created_at')                      AS pull_created_at,
    -- JSONExtractDateTime(payload, 'pull_request.updated_at')                      AS pull_updated_at,
    -- JSONExtractDateTime(payload, 'pull_request.closed_at')                       AS pull_closed_at,

    -- JSONExtractBool   (payload, 'pull_request.merged')                           AS pull_merged,
    -- JSONExtractDateTime(payload, 'pull_request.merged_at')                       AS pull_merged_at,
    -- JSONExtractString (payload, 'pull_request.merge_commit_sha')                 AS pull_merge_commit_sha,
    -- JSONExtractUInt   (payload, 'pull_request.merged_by.id')                     AS pull_merged_by_id,
    -- JSONExtractString (payload, 'pull_request.merged_by.login')                  AS pull_merged_by_login,
    -- JSONExtractString (payload, 'pull_request.merged_by.type')                   AS pull_merged_by_type,

    -- JSONExtractUInt   (payload, 'pull_request.commits')                          AS pull_commits,
    -- JSONExtractUInt   (payload, 'pull_request.additions')                        AS pull_additions,
    -- JSONExtractUInt   (payload, 'pull_request.deletions')                        AS pull_deletions,
    -- JSONExtractUInt   (payload, 'pull_request.changed_files')                    AS pull_changed_files,
    -- JSONExtractUInt   (payload, 'pull_request.comments')                         AS pull_comments,
    -- JSONExtractUInt   (payload, 'pull_request.review_comments')                  AS pull_review_comments,

    -- JSONExtractString (payload, 'pull_request.base.ref')                         AS pull_base_ref,
    -- JSONExtractString (payload, 'pull_request.head.ref')                         AS pull_head_ref,
    -- JSONExtractString (payload, 'pull_request.base.sha')                         AS pull_base_sha,
    -- JSONExtractString (payload, 'pull_request.head.sha')                         AS pull_head_sha,

    -- JSONExtractString (payload, 'pull_request.author_association')               AS pull_author_association,
    -- JSONExtractBool   (payload, 'pull_request.draft')                            AS pull_draft,

    -- JSONExtractUInt   (payload, 'pull_request.requested_reviewer.id')            AS pull_requested_reviewer_id,
    -- JSONExtractString (payload, 'pull_request.requested_reviewer.login')         AS pull_requested_reviewer_login,
    -- JSONExtractString (payload, 'pull_request.requested_reviewer.type')          AS pull_requested_reviewer_type,

    ---------------------------------------------------------
    -- PULL REQUEST REVIEW COMMENT (PullRequestReviewCommentEvent)
    ---------------------------------------------------------
    -- JSONExtractUInt   (payload, 'comment.id')                                    AS pull_review_comment_id,
    -- JSONExtractString (payload, 'comment.body')                                  AS pull_review_comment_body,
    -- JSONExtractString (payload, 'comment.path')                                  AS pull_review_comment_path,
    -- JSONExtractInt    (payload, 'comment.position')                              AS pull_review_comment_position,
    -- JSONExtractString (payload, 'comment.commit_id')                             AS pull_review_comment_commit_id,
    -- JSONExtractString (payload, 'comment.original_commit_id')                    AS pull_review_comment_original_commit_id,
    -- JSONExtractDateTime(payload, 'comment.created_at')                           AS pull_review_comment_created_at,
    -- JSONExtractDateTime(payload, 'comment.updated_at')                           AS pull_review_comment_updated_at,
    -- JSONExtractString (payload, 'comment.author_association')                    AS pull_review_comment_author_association,
    -- JSONExtractUInt   (payload, 'comment.user.id')                               AS pull_review_comment_author_id,
    -- JSONExtractString (payload, 'comment.user.login')                            AS pull_review_comment_author_login,
    -- JSONExtractString (payload, 'comment.user.type')                             AS pull_review_comment_author_type,

    ---------------------------------------------------------
    -- COMMIT COMMENT (CommitCommentEvent)
    ---------------------------------------------------------
    JSONExtractUInt   (payload, 'comment.id')                                    AS commit_comment_id,
    JSONExtractString (payload, 'comment.body')                                  AS commit_comment_body,
    JSONExtractString (payload, 'comment.path')                                  AS commit_comment_path,
    JSONExtractString (payload, 'comment.position')                              AS commit_comment_position,
    JSONExtractString (payload, 'comment.line')                                  AS commit_comment_line,
    -- JSONExtractDateTime(payload, 'comment.created_at')                           AS commit_comment_created_at,
    -- JSONExtractDateTime(payload, 'comment.updated_at')                           AS commit_comment_updated_at,
    JSONExtractString (payload, 'comment.author_association')                    AS commit_comment_author_association,
    JSONExtractUInt   (payload, 'comment.user.id')                               AS commit_comment_author_id,
    JSONExtractString (payload, 'comment.user.login')                            AS commit_comment_author_login,
    JSONExtractString (payload, 'comment.user.type')                             AS commit_comment_author_type,

    ---------------------------------------------------------
    -- PUSH (PushEvent)
    ---------------------------------------------------------
    JSONExtractUInt   (payload, 'push_id')                                       AS push_id,       -- some pipelines store this, GHArchive itself does not; feel free to drop
    JSONExtractUInt   (payload, 'size')                                          AS push_size,
    JSONExtractUInt   (payload, 'distinct_size')                                 AS push_distinct_size,
    JSONExtractString (payload, 'ref')                                           AS push_ref,
    JSONExtractString (payload, 'head')                                          AS push_head,
    JSONExtractString (payload, 'before')                                        AS push_before,

    ---------------------------------------------------------
    -- FORK (ForkEvent)
    ---------------------------------------------------------
    JSONExtractUInt   (payload, 'forkee.id')                                     AS fork_forkee_id,
    JSONExtractString (payload, 'forkee.full_name')                              AS fork_forkee_full_name,
    JSONExtractUInt   (payload, 'forkee.owner.id')                               AS fork_forkee_owner_id,
    JSONExtractString (payload, 'forkee.owner.login')                            AS fork_forkee_owner_login,
    JSONExtractString (payload, 'forkee.owner.type')                             AS fork_forkee_owner_type,

    ---------------------------------------------------------
    -- CREATE / DELETE
    ---------------------------------------------------------
    JSONExtractString (payload, 'ref')                                           AS create_ref,
    JSONExtractString (payload, 'ref_type')                                      AS create_ref_type,
    JSONExtractString (payload, 'master_branch')                                 AS create_master_branch,
    JSONExtractString (payload, 'description')                                   AS create_description,
    JSONExtractString (payload, 'pusher_type')                                   AS create_pusher_type,

    JSONExtractString (payload, 'ref')                                           AS delete_ref,
    JSONExtractString (payload, 'ref_type')                                      AS delete_ref_type,
    JSONExtractString (payload, 'pusher_type')                                   AS delete_pusher_type,

    ---------------------------------------------------------
    -- MEMBER (MemberEvent)
    ---------------------------------------------------------
    JSONExtractUInt   (payload, 'member.id')                                     AS member_id,
    JSONExtractString (payload, 'member.login')                                  AS member_login,
    JSONExtractString (payload, 'member.type')                                   AS member_type,

    ---------------------------------------------------------
    -- RELEASE (ReleaseEvent)
    ---------------------------------------------------------
    JSONExtractUInt   (payload, 'release.id')                                    AS release_id,
    JSONExtractString (payload, 'release.tag_name')                              AS release_tag_name,
    JSONExtractString (payload, 'release.target_commitish')                      AS release_target_commitish,
    JSONExtractString (payload, 'release.name')                                  AS release_name,
    JSONExtractBool   (payload, 'release.draft')                                 AS release_draft,
    JSONExtractUInt   (payload, 'release.author.id')                             AS release_author_id,
    JSONExtractString (payload, 'release.author.login')                          AS release_author_login,
    JSONExtractString (payload, 'release.author.type')                           AS release_author_type,
    JSONExtractBool   (payload, 'release.prerelease')                            AS release_prerelease,
    -- JSONExtractDateTime(payload, 'release.created_at')                           AS release_created_at,
    -- JSONExtractDateTime(payload, 'release.published_at')                         AS release_published_at,
    JSONExtractString (payload, 'release.body')                                  AS release_body

FROM file(
    'k8s-events.json.gz',
    'JSONEachRow',
    -- structure *of the raw GHArchive file*, not of github_events:
    'id String,
     type String,
     actor String,
     repo String,
     org String,
     payload String,
     created_at String'
)
SETTINGS
    input_format_skip_unknown_fields = 1;
