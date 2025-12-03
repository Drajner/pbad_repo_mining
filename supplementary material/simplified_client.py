from clickhouse_driver import Client

class ClickhouseClient:
    def __init__(self, database, host='127.0.0.1', port=9000, user='default', password='', table='year2020', agg_table='agg_year2020'):
        self.database = database
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.table = table
        self.agg_table = agg_table

    def execute(self, sql):
        client = Client(
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            database=self.database
        )
        return client.query_dataframe(sql)
    
    '''
    get the dataset overview
    '''
    def get_basic(self,where_repo_condition="", and_repo_condition=""):
        table = self.table
        agg_table = self.agg_table
        basic_sql = f'''
        SELECT COUNT(*) as records,COUNT(distinct repo_id) AS repo,COUNT(distinct actor_id) AS accounts FROM {table} {where_repo_condition}
        '''
        bots_sql = f'''
        SELECT COUNT(distinct actor_id) AS bots FROM {table} WHERE (actor_login LIKE '%[bot]' OR actor_login LIKE '%bot') {and_repo_condition}
        '''
        active_sql = f'''
        SELECT COUNT(distinct actor_id) AS active FROM {agg_table} {where_repo_condition}
        '''
        sql = f'''
        SELECT * FROM ({basic_sql}) AS basic,({active_sql}) AS active,({bots_sql}) AS bots
        '''
        rs = self.execute(sql)
        return rs
    '''
    get activity distribution
    '''
    def get_pareto(self,repo_condition="",percent_list=(0.001,0.01,0.02,0.05,0.08,0.1,0.2,0.5,0.8,1.0)):
        agg_table = self.agg_table
        actor_activity_sql = f'''
            SELECT
            SUM(score)/366 AS actor_activity 
            FROM {agg_table}
            WHERE actor_login NOT LIKE '%[bot]' AND actor_login NOT LIKE '%bot' {repo_condition}
            GROUP BY actor_id
            '''
        total_activity_actor_num_sql = f'''
                SELECT SUM(actor_activity) AS total_activity,COUNT(*) AS actor_num FROM ({actor_activity_sql})
            '''
        total_activity_actor_num = self.execute(total_activity_actor_num_sql)
        total_activity = total_activity_actor_num['total_activity'][0]
        actor_num = total_activity_actor_num['actor_num'][0]

        base_accumulation_rate_sql = ""
        order_actor_activity_sql = actor_activity_sql + "\nORDER BY actor_activity DESC"
        for percent in percent_list:
            accumulation_rate_sql = '''
                    SELECT SUM(actor_activity)/{total_activity}*100 AS accumulation_rate,{percent}* 100 AS percent FROM ({order_actor_activity_sql})
                '''.format(
                order_actor_activity_sql=order_actor_activity_sql + '\nLIMIT ' + str(max(round(actor_num * percent),1)),
                total_activity=total_activity,
                percent=percent)

            if percent != 1:
                base_accumulation_rate_sql += accumulation_rate_sql + "\nUNION ALL\n"
            else:
                base_accumulation_rate_sql += accumulation_rate_sql
        rs = self.execute(base_accumulation_rate_sql)
        return rs
    '''
    get behavior distribution about issue_comment,open_issue,open_pull,pull_review_comment,merge_pull
    '''
    def get_behavior_distribution(self,condition):
        agg_table = self.agg_table
        sql = f'''
                        SELECT 
                        SUM(issue_comment) AS ic,
                        SUM(open_issue) AS oi,
                        SUM(open_pull) AS op,
                        SUM(pull_review_comment) AS prc,
                        SUM(merge_pull) AS mp
                        FROM {agg_table}
                        {condition}
                        '''
        rs = self.execute(sql)
        return rs
    '''
    get top 10 projects with most active contributors
    '''
    def get_top_projects_ac(self,percent=0.2):
        agg_table = self.agg_table
        actor_activity_sql = f'''
        SELECT actor_id,
        SUM(score)/366 AS actor_activity 
        FROM {agg_table}
        WHERE actor_login NOT LIKE '%[bot]' AND actor_login NOT LIKE '%bot'
        GROUP BY actor_id
        '''
        actor_num_sql = f'''
            SELECT round(COUNT(*)*{percent}) AS actor_num FROM ({actor_activity_sql})
        '''
        order_actor_activity_sql = actor_activity_sql + f"\nORDER BY actor_activity DESC\nLIMIT ({actor_num_sql})"
        top_actor_ids_sql = f'''
            SELECT actor_id FROM ({order_actor_activity_sql})
        '''
        repo_actor_sql = f'''
        SELECT repo_id,anyHeavy(repo_name) AS repo_name,actor_id FROM {agg_table} GROUP BY repo_id,actor_id HAVING actor_id IN ({top_actor_ids_sql})
        '''
        top_repo_sql = f'''
        SELECT anyHeavy(repo_name) AS repo_name,COUNT(*) AS c FROM ({repo_actor_sql}) GROUP BY repo_id ORDER BY c DESC LIMIT 10
        '''
        rs=self.execute(top_repo_sql)
        return rs
    '''
    get working hour distribution
    '''
    def get_day_week_activity(self,repo_condition=""):
        agg_table = self.agg_table
        sql = f'''
        SELECT hour,week,SUM(score)/366 AS c FROM {agg_table} WHERE actor_login NOT LIKE '%[bot]' AND actor_login NOT LIKE '%bot' {repo_condition} GROUP BY hour,week
        '''
        rs = self.execute(sql)
        return rs
    '''
    get working hour distribution on their local time
    '''
    def get_day_week_activity_based_time_zone(self,repo_condition="", percent=0.2, work_hours=8, end_cur_hour=18):
        agg_table = self.agg_table
        actor_activity_sql = f'''
        SELECT actor_id,
        SUM(score)/366 AS actor_activity
        FROM {agg_table}
        WHERE actor_login NOT LIKE '%[bot]' AND actor_login NOT LIKE '%bot' {repo_condition}
        GROUP BY actor_id
        '''
        actor_num_sql = f'''
            SELECT round(COUNT(*)*{percent}) AS actor_num FROM ({actor_activity_sql})
        '''
        order_actor_activity_sql = actor_activity_sql + f"\nORDER BY actor_activity DESC\nLIMIT ({actor_num_sql})"
        actor_ids = f'''
            SELECT actor_id FROM ({order_actor_activity_sql})
        '''
        actor_hour_activity_sql = f'''
        SELECT actor_id,hour,SUM(score)/366 AS activity FROM {agg_table} GROUP BY actor_id,hour having actor_id in ({actor_ids})
        '''
        actor_hours_activity_sql = f'''
            SELECT actor_id,hour,SUM(activity) AS activity FROM
              (SELECT b.actor_id AS actor_id,b.hour AS hour,a.activity AS activity
              FROM ({actor_hour_activity_sql}) AS a,({actor_hour_activity_sql}) AS b
              WHERE b.actor_id=a.actor_id AND b.hour-a.hour<{work_hours} AND b.hour-a.hour>=0
              UNION ALL
              SELECT b.actor_id AS actor_id,b.hour AS hour,a.activity AS activity
              FROM ({actor_hour_activity_sql}) AS a,({actor_hour_activity_sql}) AS b
              WHERE b.actor_id=a.actor_id AND b.hour-a.hour+24<{work_hours})
            GROUP BY actor_id,hour
        '''
        cur_hour=end_cur_hour - 1
        actor_zone_sql = f'''
            SELECT actor_id,if({cur_hour}-argMax(hour,activity)<12,{cur_hour}-argMax(hour,activity),{cur_hour}-24-argMax(hour,activity)) AS zone FROM ({actor_hours_activity_sql})
            GROUP BY actor_id
        '''
        weekday_time_sql = f'''
            SELECT
            if((hour+zone)>23,if((week+1)>7,1,week+1),if((hour+zone)<0,if((week-1)<1,7,week-1),week)) AS weekday,
            if((hour+zone)>23,(hour+zone)-24,if((hour+zone)<0,(hour+zone)+24,hour+zone)) AS time,
            score
            FROM {agg_table} AS a,({actor_zone_sql}) AS b WHERE a.actor_id=b.actor_id
        '''
        sql = f'''
            SELECT time AS hour,weekday AS week,SUM(score)/366 AS c FROM ({weekday_time_sql}) GROUP BY hour,week ORDER BY week,hour
        '''
        rs = self.execute(sql)
        return rs
    '''
    get time zone distribution of contributors
    '''
    def get_time_zone(self, repo_condition="", percent=0.2, work_hours=8, end_cur_hour=18):
        agg_table = 'agg_year2020'
        actor_activity_sql = f'''
        SELECT actor_id,
        SUM(score)/366 AS actor_activity 
        FROM {agg_table}
        WHERE actor_login NOT LIKE '%[bot]' AND actor_login NOT LIKE '%bot' {repo_condition}
        GROUP BY actor_id
        '''
        actor_num_sql = f'''
            SELECT round(COUNT(*)*{percent}) AS actor_num FROM ({actor_activity_sql})
        '''
        order_actor_activity_sql = actor_activity_sql + f"\nORDER BY actor_activity DESC\nLIMIT ({actor_num_sql})"
        actor_ids = f'''
            SELECT actor_id FROM ({order_actor_activity_sql})
        '''
        actor_hour_activity_sql = f'''
        SELECT actor_id,hour,SUM(score)/366 AS activity FROM {agg_table} GROUP BY actor_id,hour having actor_id in ({actor_ids})
        '''
        actor_hours_activity_sql = f'''
            SELECT actor_id,hour,SUM(activity) AS activity FROM
              (SELECT b.actor_id AS actor_id,b.hour AS hour,a.activity AS activity 
              FROM ({actor_hour_activity_sql}) AS a,({actor_hour_activity_sql}) AS b
              WHERE b.actor_id=a.actor_id AND b.hour-a.hour<{work_hours} AND b.hour-a.hour>=0
              UNION ALL
              SELECT b.actor_id AS actor_id,b.hour AS hour,a.activity AS activity 
              FROM ({actor_hour_activity_sql}) AS a,({actor_hour_activity_sql}) AS b
              WHERE b.actor_id=a.actor_id AND b.hour-a.hour+24<{work_hours})
            GROUP BY actor_id,hour
        '''
        actor_zone_sql = f'''
            SELECT actor_id,argMax(hour,activity) AS hour FROM ({actor_hours_activity_sql})
            GROUP BY actor_id
        '''
        cur_hour = end_cur_hour - 1
        sql = f'''
        SELECT if({cur_hour}-hour<12,{cur_hour}-hour,{cur_hour}-24-hour) AS zone,count(*) AS c FROM ({actor_zone_sql})
              GROUP BY zone
              ORDER BY zone
        '''
        rs = self.execute(sql)
        return rs