The supplementary materials are for the paper `A Large Scale Exploration of GitHub Activity: Who, What, When, and Where`

You can see the structure of raw data in the **data description** folder, the processed intermediate data in the **data** folder. Due to the sizable data volume we used, we don't provide the complete records here, but they can be obtained from the *GHArchive* project.

All the images can be found in the **imgs** folder.

Here are descriptions of the Python script files:

- `client.py`: the client for getting data from clickhouse server
- `proxy.py`:the proxy for clickhouse client which helps to get all data about our paper. In view of safety considerations,we mask the clickhouse config.
- `cncf_config.py`:the config about cncf repos including repo_id,repo_name,status
- `cncf_commit.py`:the file for dealing cncf commit data
- `visualize.py`:the file for visualization

# DB setup:

- Run docker compose: `docker compose up`
- Fill db with schema.sql: `cat schema.sql | docker exec -i clickhouse clickhouse-client -n`
- Fill db with data: `cat sample_data.sql | docker exec -i clickhouse clickhouse-client -n`

# UPLOADING DATA TO CLICKHOUSE DB:

inside IDE , \supplementary_material package:
1. `docker cp k8s-events.json.gz clickhouse:/var/lib/clickhouse/user_files/k8s-events.json.gz`
2. Wejdz do clickhouse client wewnatrz kontenera: `docker exec -it clickhouse clickhouse-client`
3. Sprawdzic czy załadowały się dane: 
    ```
    SHOW DATABASES;
    USE github_log;
    SHOW TABLES;
    DESCRIBE TABLE year2020;
4. Odpalić skrypt tworzacy tabele year2020:  `Get-Content insert_file.sql | docker exec -i clickhouse clickhouse-client --multiquery`
5. Wejdz do clickhouse client wewnatrz kontenera: `docker exec -it clickhouse clickhouse-client`
6. Sprawdzić czy się utworzyła tabela:
   ```
    SELECT count(*) FROM year2020;
    SELECT type, count() 
    FROM year2020
    GROUP BY type
    ORDER BY count() DESC
    LIMIT 10;
7. Odpalić skrypt tworzacy wersje zagregowana tabeli year2020: ` Get-Content .\agg_table.sql | docker exec -i clickhouse clickhouse-client --multiquery`
8. Wejdz do clickhouse client wewnatrz kontenera: `docker exec -it clickhouse clickhouse-client`
9. Sprawdzić czy się utworzyła tabela:
   ```
    SELECT count() FROM agg_year2020;
    SELECT actor_login, sum(score) AS total_score
    FROM agg_year2020
    GROUP BY actor_login
    ORDER BY total_score DESC
    LIMIT 10;
 
# DATA ANALYSIS
run:
1. proxy.py
2. visualize.py