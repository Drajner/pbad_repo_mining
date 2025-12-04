# Data Description

## GitHub Event Log

### Data Source

The data source comes from [GH Archive](https://www.gharchive.org/) which is a project to record the public GitHub timeline, archive it and make it easily accessible for further analysis. Each archive contains JSON encoded events as reported by the GitHub API. The raw JSON data is showing below. There are 6 important data features in this data, namely `id`, `type`, `actor`, `repo`, `payload`, `created_at`.

![](./gharchive_raw_data.png)

### Database

In order to meet the requirement for high-speed analysis among such big data, we parse the row data into well-defined structure and import it into [ClickHouse](https://clickhouse.tech/) server which is an open source column-oriented database management system capable of real time generation of analytical data reports using SQL queries. The Clickhouse database version is 20.8.7.15 in our server. 

### Data Schema in Database

The database table offered by the `Clickhouse` server is showing in [data description](./data_description.csv). 
You can find a table called `year2020` with 120+ rows of features which were parsed from the raw GHArchive datasets. 

In order to speed up the analysis, we used a temporary table called `agg_year2020` which aggregate from `year2020`.
You can see the [agg description](./agg_description.csv) about this.