import os
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

def visualize_activity_distribution(absolute_path):
    fontsize = 15
    data_path = absolute_path + '/data/pareto.csv'
    df=pd.read_csv(data_path,index_col=0)
    df['percent']=df['percent'].astype('str')
    plt.plot(df['percent'],df['all'],color='#577590')
    plt.xlabel('proportion of contributors',fontsize=fontsize)
    plt.ylabel('cumulative proportion of activity',fontsize=fontsize)
    plt.xticks(fontsize=fontsize)
    plt.yticks(fontsize=fontsize)
    plt.tight_layout()
    save_path = absolute_path + '/imgs/pareto/all.png'
    plt.savefig(fname=save_path)
    plt.figure()

'''
behavior distribution
'''
def visualize_behavior_distribution(absolute_path):
    data_path = absolute_path + '/data/behavior_distribution.csv'
    df=pd.read_csv(data_path,index_col=0)
    fontsize = 15
    df=df/10000
    x = np.arange(len(df.columns))
    width = 0.35
    fig, ax = plt.subplots()
    ax.bar(x - width/2, df.loc['all_ac'], width, label='AC',color='#c9ada7')
    ax.bar(x + width/2, df.loc['all_bot'], width, label='bot',color='#6d6875')
    ax.set_xlabel('behavior',fontsize=fontsize)
    ax.set_ylabel('count(×10000)',fontsize=fontsize)
    ax.set_xticks(x)
    ax.set_xticklabels(df.columns,fontsize=fontsize)
    plt.yticks(fontsize=fontsize)
    ax.legend(fontsize=fontsize)
    plt.tight_layout()
    save_path = absolute_path + '/imgs/behavior_distribution/what-all.png'
    plt.savefig(fname=save_path)
'''
working hour distribution
'''
def visualize_working_hour_distribution(absolute_path):
    def helper(name):
        fontsize = 15
        data_path = absolute_path + '/data/day_week_activity.csv'
        df = pd.read_csv(data_path,index_col=0)
        x = df['hour'].values
        y = df['week'].values
        size = ((df[name].values - np.min(df[name].values)) / (np.max(df[name].values) - np.min(df[name].values))) * 200

        plt.figure(figsize=(10.2, 3.4))
        plt.scatter(x, y, s=size, c=size,cmap='Blues')
        ax = plt.gca()
        ax2 = ax.twiny()
        ax2.spines['right'].set_color('none')
        ax2.spines['top'].set_color('none')
        ax2.spines['top'].set_position(('data', -0.1))
        ax2.spines['left'].set_color('none')
        ax2.spines['bottom'].set_color('none')
        ax2.xaxis.set_ticks_position('top')
        ax2.xaxis.set_ticks(range(0, 24))
        ax2.set_xticklabels([8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 0, 1, 2, 3, 4, 5, 6, 7],fontsize=fontsize)
        ax.text(-2.5, -0.52, "UTC+8",fontsize=fontsize)
        ax2.tick_params(bottom=False, top=False, left=False, right=False)
        plt.scatter(x, y, s=size, c=size,cmap='Greens')
        ax3 = ax.twiny()
        ax3.spines['right'].set_color('none')
        ax3.spines['top'].set_color('none')
        ax3.spines['top'].set_position(('data', -0.9))
        ax3.spines['left'].set_color('none')
        ax3.spines['bottom'].set_color('none')
        ax3.xaxis.set_ticks_position('top')
        ax3.xaxis.set_ticks(range(0, 24))
        ax3.set_xticklabels(range(0, 24),fontsize=fontsize)
        ax.text(-2.5, -1.35, "UTC",fontsize=fontsize)
        ax3.tick_params(bottom=False, top=False, left=False, right=False)
        plt.scatter(x, y, s=size, c=size,cmap='Greens')
        ax.spines['right'].set_color('none')
        ax.spines['top'].set_color('none')
        ax.spines['left'].set_color('none')
        ax.spines['bottom'].set_color('none')
        ax.xaxis.set_ticks_position('top')
        ax.xaxis.set_ticks(range(0, 24))
        ax.set_xticklabels([19, 20, 21, 22, 23, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18],fontsize=fontsize)
        ax.text(-2.5, 0.29, "UTC-5",fontsize=fontsize)
        ax.yaxis.set_ticks_position('left')
        ax.invert_yaxis()
        ax.set_yticks(range(1, 8))
        ax.set_yticklabels(['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun'], fontsize=fontsize)
        ax.tick_params(bottom=False, top=False, left=False, right=False)
        plt.tight_layout()
        save_path = absolute_path + f'/imgs/working_hour_distribution/{name}.png'
        plt.savefig(fname=save_path)
    for name in ['all']:
        helper(name)
'''
working hour distribution on their local time
'''
def visualize_working_hour_distribution_local_time(absolute_path):
    fontsize = 15
    data_path = absolute_path + '/data/day_week_activity_based_time_zone.csv'
    df=pd.read_csv(data_path,index_col=0)
    values=df[['hour','week','all']].values
    week_data={
        'Mon':[0.0]*24,
        'Tue':[0.0]*24,
        'Wed':[0.0]*24,
        'Thu':[0.0]*24,
        'Fri':[0.0]*24,
        'Sat':[0.0]*24,
        'Sun':[0.0]*24
    }
    for hour,week,count in values:
        hour=int(hour)
        week=int(week)
        if week==1:
            week_data['Mon'][hour]=count
        if week == 2:
            week_data['Tue'][hour] = count
        if week == 3:
            week_data['Wed'][hour] = count
        if week == 4:
            week_data['Thu'][hour] = count
        if week == 5:
            week_data['Fri'][hour] = count
        if week == 6:
            week_data['Sat'][hour] = count
        if week==7:
            week_data['Sun'][hour]=count
    plt.figure(figsize=(10.2, 5.1))
    for k in week_data:
        plt.plot(range(0,24),week_data[k],label=k)
    plt.xticks(range(0,24),fontsize=fontsize)
    plt.yticks(fontsize=fontsize)
    plt.xlabel('hour',fontsize=fontsize)
    plt.ylabel('total activity of AC',fontsize=fontsize)
    plt.legend(fontsize=fontsize)
    plt.tight_layout()
    save_path = absolute_path + f'/imgs/working_hour_distribution_local_time/all.png'
    plt.savefig(fname=save_path)

'''
time zone distribution
'''
def visualize_time_zone_distribution(absolute_path):
    data_path = absolute_path + '/data/time_zone.csv'
    df=pd.read_csv(data_path,index_col=0)
    fontsize = 15
    plt.figure(figsize=(10.2, 5.1))
    activity_num=df['all'].values
    activity = activity_num / sum(activity_num) * 100
    plt.bar(df['zone'],activity,color='#c9ada7')
    plt.xticks(range(-12,12),fontsize=fontsize)
    plt.yticks(fontsize=fontsize)
    plt.xlabel('time zone',fontsize=fontsize)
    plt.ylabel('% of AC',fontsize=fontsize)
    plt.text(-12, 7, "variance: " + str(round(activity.std() ** 2, 2)),fontsize=fontsize)
    plt.tight_layout()
    save_path = absolute_path + f'/imgs/time_zone_distribution/all.png'
    plt.savefig(fname=save_path)


if __name__ == '__main__':
    absolute_path = os.path.dirname(os.path.abspath(__file__))
    visualize_activity_distribution(absolute_path)
    visualize_behavior_distribution(absolute_path)
    visualize_working_hour_distribution(absolute_path)
    visualize_working_hour_distribution_local_time(absolute_path)
    visualize_time_zone_distribution(absolute_path)