import pandas as pd
from simplified_client import ClickhouseClient
from cncf_configs import cncf_repos
'''
the proxy class for getting data and saving data
'''
class DataProxy:
    def __init__(self,client:ClickhouseClient):
        self.client=client
        self.cncf_ids = [repo['id'] for repo in cncf_repos]
        self.graduated_ids = [repo['id'] for repo in cncf_repos if repo['status'] == 'graduated']
        self.incubating_ids = [repo['id'] for repo in cncf_repos if repo['status'] == 'incubating']
        self.sandbox_ids = [repo['id'] for repo in cncf_repos if repo['status'] == 'sandbox']
        self.k8s_id=20580498
        self.tikv_id = 48833910
        self.repo_conditions=[
            ('all', ""),
            ('cncf', f"AND repo_id in {self.cncf_ids}"),
            ('graduated', f"AND repo_id in {self.graduated_ids}"),
            ('incubating', f"AND repo_id in {self.incubating_ids}"),
            ('sandbox', f"AND repo_id in {self.sandbox_ids}"),
            ('kubernetes', f"AND repo_id={self.k8s_id}"),
            ('tikv', f"AND repo_id={self.tikv_id}")
        ]
    def get_basic_proxy(self):
        repo_conditions = [('all', "", ""),
                           ('cncf', f"WHERE repo_id in {self.cncf_ids}", f"AND repo_id in {self.cncf_ids}"),
                           ('graduated', f"WHERE repo_id in {self.graduated_ids}", f"AND repo_id in {self.graduated_ids}"),
                           ('incubating', f"WHERE repo_id in {self.incubating_ids}", f"AND repo_id in {self.incubating_ids}"),
                           ('sandbox', f"WHERE repo_id in {self.sandbox_ids}", f"AND repo_id in {self.sandbox_ids}")]
        index=[]
        data=[]
        for name, where_repo_condition, and_repo_condition in repo_conditions:
            index.append(name)
            data.append(self.client.get_basic(where_repo_condition, and_repo_condition).values[0])
        rs= pd.DataFrame(data,index=index,columns=['record','repo.','account','contrib.','bot'])
        return rs
    def get_pareto_proxy(self):
        repo_conditions = self.repo_conditions
        df=pd.DataFrame()
        df['percent']=[str(i*100) for i in (0.001,0.01,0.02,0.05,0.08,0.1,0.2,0.5,0.8,1)]
        for name, repo_condition in repo_conditions:
            rs = self.client.get_pareto(repo_condition=repo_condition)
            rs = rs.sort_values(by='percent', ascending=True)
            df[name]=rs['accumulation_rate'].values
        return df
    def get_behavior_distribution_proxy(self):
        repo_conditions = self.repo_conditions
        index=[]
        data=[]
        for name,repo_condition in repo_conditions:
            bot_info=self.client.get_behavior_distribution("WHERE (actor_login LIKE '%[bot]' OR actor_login LIKE '%bot') " + repo_condition)
            ac_info=self.client.get_behavior_distribution("WHERE actor_login NOT LIKE '%[bot]' AND actor_login NOT LIKE '%bot' "+repo_condition)
            index+=[name+'_bot',name + '_ac']
            data+=[bot_info.values[0],ac_info.values[0]]
        rs=pd.DataFrame(data,index=index,columns=['c','oi','op','rpr','mp'])
        return rs
    def get_day_week_activity_proxy(self):
        repo_conditions = [
            self.repo_conditions[0],
            self.repo_conditions[-2],
            self.repo_conditions[-1]
        ]
        df=pd.DataFrame()
        df['hour']=[j for _ in range(1, 8) for j in range(24)]
        df['week'] = [i for i in range(1, 8) for _ in range(24)]
        for name,repo_condition in repo_conditions:
            data = [0 for _ in range(1, 8) for _ in range(24)]
            rs = self.client.get_day_week_activity(repo_condition=repo_condition)
            for hour,week,activity in rs.values:
                data[int(hour+(week-1)*24)]=activity
            df[name]=data
        return df
    def get_day_week_activity_based_time_zone_proxy(self):
        repo_conditions = self.repo_conditions
        df = pd.DataFrame()
        df['hour'] = [j for _ in range(1, 8) for j in range(24)]
        df['week'] = [i for i in range(1, 8) for _ in range(24)]
        for name, repo_condition in repo_conditions:
            data = [0 for _ in range(1, 8) for _ in range(24)]
            rs = self.client.get_day_week_activity_based_time_zone(repo_condition=repo_condition)
            for hour, week, activity in rs.values:
                data[int(hour + (week - 1) * 24)] = activity
            df[name] = data
        return df
    def get_time_zone_proxy(self):
        repo_conditions = self.repo_conditions
        df=pd.DataFrame()
        df['zone']=[str(i) for i in range(-12,12)]
        for name,repo_condition in repo_conditions:
            data=[0 for _ in range(-12,12)]
            rs=self.client.get_time_zone(repo_condition=repo_condition)
            for zone,c in rs.values:
                data[zone+12]=c
            df[name]=data
        return df
    def get_data(self):
        self.get_basic_proxy().to_csv('data/basic.csv')
        self.get_pareto_proxy().to_csv('data/pareto.csv')
        self.get_behavior_distribution_proxy().to_csv('data/behavior_distribution.csv')
        self.client.get_top_projects_ac().to_csv('data/top_projects_ac.csv')
        self.get_day_week_activity_proxy().to_csv('data/day_week_activity.csv')
        self.get_day_week_activity_based_time_zone_proxy().to_csv('data/day_week_activity_based_time_zone.csv')
        self.get_time_zone_proxy().to_csv('data/time_zone.csv')

if __name__ == '__main__':
    '''
    the config for clickhouse server
    '''

    """
    server_address = "*"
    server_port = 22
    server_user_name = "*"
    server_user_password = "*"
    remote_address = "*"
    remote_port = 9000
    remote_user_name = '*'
    remote_user_passward = '*'
    database = 'github_log'
    cc = ClickhouseClient(
        server_address, server_port,
        server_user_name, server_user_password,
        remote_address, remote_port,
        remote_user_name, remote_user_passward,
        database)
    """

    database = 'github_log'
    cc = ClickhouseClient(database=database, user='script_user', password='generic_password')
    dp = DataProxy(cc)
    dp.get_data()
