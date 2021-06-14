import time
import pandas as pd

from utils.csv2excel import csv_to_xls

mgr_detail = pd.read_csv('../data/2021-06-14-10-37-43.csv')
daily_profit = pd.read_csv('../data/avg_daily_income_2021-06-14-08-30-09.csv')

df = pd.merge(mgr_detail, daily_profit, how='right', on='id', suffixes=('_x', ''))
df = df.filter(
    items=['id', 'name', 'avg_daily_income', 'at_present_total_size', 'job_duration_days','at_present_best_profit', 'at_present_fouds_num'])
# data = []
# for index, row in daily_profit.iterrows():
#     mgr_info = mgr_detail[mgr_detail['id'] == row['id']]
#     data.append([row['id'], row['name'], row['avg_daily_income'], mgr_info['at_present_total_size'].values,
#                  mgr_info['at_present_best_profit'].values, mgr_info['at_present_fouds_num'].values])
#
# df = pd.DataFrame(data, columns=['id', 'name', 'avg_daily_income', 'at_present_total_size', 'at_present_best_profit',
#                                  'at_present_fouds_num'])
df = df.sort_values(by='avg_daily_income', ascending=False)
fie_path = time.strftime("%Y-%m-%d-%H-%M-%S", time.localtime())
df.to_csv(fie_path + '.csv')

csv_to_xls(fie_path+ '.csv', fie_path + '.xls')
