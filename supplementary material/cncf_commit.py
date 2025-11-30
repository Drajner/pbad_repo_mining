'''
As for cncf commit data, we just use 'git clone' from github to get 'git log'.
For example, we show the shell process for kubernetes 'git log'.
1. git clone git@github.com:kubernetes/kubernetes.git
2. cd kubernetes && git log --after "2020-01-01 00:00:00" --before "2021-01-01 00:00:00" --date=iso --pretty=format:"%an"SEPARATOR"%ad" >> commit.csv'
one csv data e.g. dependabot[bot]SEPARATOR2020-12-31 04:47:57 +0000
For all cncf data,we use a script to get them.
And then we aggregate the time zone and the number of developer who hava commits. The result data is in data/commits
'''
from cncf_configs import cncf_repos
import pandas as pd
def deal_commits(names):
    base_name = names[0]
    base = pd.read_csv(f'data/commits/{base_name}.csv')
    base.columns = ['zone', base_name]
    for repo_name in names[1:]:
        repo = pd.read_csv(f'data/commits/{repo_name}.csv')
        repo.columns = ['zone', repo_name]
        base = pd.merge(base, repo, on='zone', how='outer')
    base = base.fillna(value=0)
    for repo_name in names[1:]:
        base[base_name] += base[repo_name]
    base = base[['zone', base_name]]
    base.columns = ['zone', 'num']
    base['num'] = base['num'].astype("int")
    commit_num = {str(i): 0 for i in range(-12, 12)}
    for zone, num in base.values:
        index = zone // 100
        remain = zone % 100
        if remain == 30:
            commit_num[str(index)] = round(num / 2)
            if index < 0:
                commit_num[str(index - 1)] = round(num / 2)
            else:
                commit_num[str(index + 1)] = round(num / 2)
        elif remain < 30:
            if index != 13 and index != 12:
                commit_num[str(index)] = num
            else:
                commit_num['-12'] += num
        else:
            if index < 0:
                commit_num[str(index - 1)] = num
            else:
                commit_num[str(index + 1)] = num
    df=pd.Series(commit_num)
    return df

def deal_commits_proxy():
    cncf_names=[repo['name'].split('/')[1] for repo in cncf_repos]
    graduated_names=[repo['name'].split('/')[1] for repo in cncf_repos if repo['status']=='graduated']
    incubating_names=[repo['name'].split('/')[1] for repo in cncf_repos if repo['status']=='incubating']
    sandbox_names=[repo['name'].split('/')[1] for repo in cncf_repos if repo['status']=='sandbox']
    commit_names=[
        ('cncf',cncf_names),
        ('graduated',graduated_names),
        ('incubating',incubating_names),
        ('sandbox',sandbox_names),
        ('kubernetes',['kubernetes']),
        ('tikv',['tikv'])
    ]
    df=pd.DataFrame()
    for name,names in commit_names:
        se=deal_commits(names)
        df[name]=se
    df.to_csv('data/commit_time_zone.csv')

if __name__ == '__main__':
    deal_commits_proxy()
