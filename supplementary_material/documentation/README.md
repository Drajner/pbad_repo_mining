The supplementary materials are for the paper `A Large Scale Exploration of GitHub Activity: Who, What, When, and Where`

You can see the structure of raw data in the **data description** folder, the processed intermediate data in the **data** folder. Due to the sizable data volume we used, we don't provide the complete records here, but they can be obtained from the *GHArchive* project.

All the images can be found in the **imgs** folder.

Here are descriptions of the Python script files:

- `client.py`: the client for getting data from clickhouse server
- `proxy.py`:the proxy for clickhouse client which helps to get all data about our paper. In view of safety considerations,we mask the clickhouse config.
- `cncf_config.py`:the config about cncf repos including repo_id,repo_name,status
- `cncf_commit.py`:the file for dealing cncf commit data
- `visualize.py`:the file for visualization

---------------------------------------------------------------------

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


# build_file.py 
This script receives github events from last 3 months and creates a file in GHArchive structure

* GET https://api.github.com/repos/{OWNER}/{REPO}/events

# RESULTS

* imgs/pareto - sprawdzają zasadę 20/80
     osie:
        1. x: procent kontrybutorów
        2. Y: skumulowany procent aktywności

  - all.png: 
  - cncf.png: projekty, które są hostowane lub wspierane w ramach CNCF, podzielone na 3 „poziomy dojrzałości”
  - projects.png: porównanie konkretnych projektów - domyślnie tikv vs kubernetes (mniejszy vs duży)

* imgs/behavior_distribution
    kolumny:
  1. c -> comment
  2. oi -> open issue
  3. op -> open pull request
  4. rpr -> review pull request
  5. mp -> merge pull request
  6. AC -> humans
  7. bots
  
     - what-all.png: ludzie i boty
     - what-cncf.png: ludzie i boty
     - what-maturity.png: tylko ludzie
     - what-project.png: tylko ludzie