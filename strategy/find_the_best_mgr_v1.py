import time
import pandas as pd
from utils.csv2excel import csv_to_xls

mgr_detail = pd.read_csv('../data/2021-06-17.csv')
daily_profit = pd.read_csv('../data/avg_daily_income_2021-06-21-22-40-49.csv')

df = pd.merge(mgr_detail, daily_profit, how='right', on='id', suffixes=('_x', ''))
df = df.filter(
    items=['id', 'name', 'avg_daily_income', 'valid_fund_num', 'recent_a_week',
           'recent_one_month_profit', 'recent_three_month_profit',
           'recent_half_a_year_profit', 'recent_a_year_profit', 'at_present_total_size', 'job_duration_days',
           'at_present_best_profit',
           'at_present_fouds_num'])

df = df.sort_values(by='avg_daily_income', ascending=False)
fie_path = time.strftime("%Y-%m-%d-%H-%M-%S", time.localtime())
df.to_csv(fie_path + '.csv')

csv_to_xls(fie_path + '.csv', fie_path + '.xls')
