The supplementary materials are for the paper `A Large Scale Exploration of GitHub Activity: Who, What, When, and Where`

You can see the structure of raw data in the **data description** folder, the processed intermediate data in the **data** folder. Due to the sizable data volume we used, we don't provide the complete records here, but they can be obtained from the *GHArchive* project.

All the images can be found in the **imgs** folder.

Here are descriptions of the Python script files:

- `client.py`: the client for getting data from clickhouse server
- `proxy.py`:the proxy for clickhouse client which helps to get all data about our paper. In view of safety considerations,we mask the clickhouse config.
- `cncf_config.py`:the config about cncf repos including repo_id,repo_name,status
- `cncf_commit.py`:the file for dealing cncf commit data
- `visualize.py`:the file for visualization



