import heapq
import logging
import re
import time
import utils.timestamp
import pandas as pd
from crawl.fast_crawl_foud_detail import get_data_by_code

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
data = pd.read_csv('../data/2021-06-17.csv')

result = []
top5 = []


def calc_info_by_code(fund_code):
    dic = get_data_by_code(fund_code)
    is_hb = dic['is_hb']
    if is_hb:
        logging.info('基金' + str(fund_code) + '是货币基金')
        return [], True
    trend = dic['worth_trend']
    sz_trend = len(trend)
    if sz_trend < 2:
        logging.info('基金' + str(fund_code) + ' sz < 2')
        return {}, True
    if trend[0][1] is None or trend[sz_trend - 1][1] is None or trend[0][0] is None:
        logging.info('基金' + str(fund_code) + ' is None')
        return {}, True
    establish_date = trend[0][0]
    total_profit = (trend[sz_trend - 1][1] - trend[0][1]) / trend[0][1]
    daily_profit = float(total_profit) / float(utils.timestamp.get_days_from_now(establish_date))
    recent_a_year_profit, _ = calc_recently_profit(trend, 365)
    recent_half_a_year_profit, _ = calc_recently_profit(trend, 365 // 2)
    recent_one_month_profit, _ = calc_recently_profit(trend, 30)
    recent_a_week, _ = calc_recently_profit(trend, 7)
    recent_three_month_profit, _ = calc_recently_profit(trend, 90)
    res = {
        "fund_code": fund_code,
        "name": dic['name'],
        "establish_date": dic['start_date'],
        "total_profit": fund_code,
        "daily_profit": daily_profit,
        "recent_a_week": recent_a_week,
        "recent_one_month_profit": recent_one_month_profit,
        "recent_three_month_profit": recent_three_month_profit,
        "recent_half_a_year_profit": recent_half_a_year_profit,
        "recent_a_year_profit": recent_a_year_profit,
    }
    return res, False


def calc_recently_profit(trend, days):
    sz_trend = len(trend)
    try:
        recent_date = int(utils.timestamp.get_timestamp_before_now(days) * 1000)
        pos = binary_search(trend, recent_date, 0, sz_trend - 1)
        from_date = trend[pos][0]
        delta_days = utils.timestamp.get_days_from_now(from_date)
        if abs(delta_days - days) > (days / 4):
            return 0.0, True

        profit = (trend[sz_trend - 1][1] - trend[pos][1]) / trend[pos][1]
        avg_profit = float(profit) / float(delta_days)
    except Exception as e:
        return 0.0, True
    return avg_profit, False


def binary_search(trend, target, left, right):
    mid = (left + right) // 2
    val = trend[mid][0]
    if target > val:
        if mid + 1 >= right:
            return right
        return binary_search(trend, target, mid + 1, right)
    if target < val:
        if left >= mid - 1:
            return left
        return binary_search(trend, target, left, mid - 1)
    return mid


def safe_div(n):
    if n[1] <= 0:
        return 0.0
    return n[0] / n[1]


def add_when_not_zero(n, a):
    if a == 0.0:
        n[1] = n[1]-1
        return n[0]
    return n[0] + a


try:
    for index, row in data.iterrows():
        fouds_id = row['at_present_fouds_id']
        codes = re.findall(r"[0-9]+", fouds_id)
        sz = len(codes)
        avg_daily_income = [0.0, sz]
        recent_a_week = [0.0, sz]
        recent_one_month_profit = [0.0, sz]
        recent_three_month_profit = [0.0, sz]
        recent_half_a_year_profit = [0.0, sz]
        recent_a_year_profit = [0.0, sz]
        for code in codes:
            d, isInvalid = calc_info_by_code(code)
            if isInvalid:
                avg_daily_income[1] = avg_daily_income[1] - 1
                recent_a_week[1] = recent_a_week[1] - 1
                recent_one_month_profit[1] = recent_one_month_profit[1] - 1
                recent_three_month_profit[1] = recent_three_month_profit[1] - 1
                recent_half_a_year_profit[1] = recent_half_a_year_profit[1] - 1
                recent_a_year_profit[1] = recent_a_year_profit[1] - 1
                continue
            avg_daily_income[0] = avg_daily_income[0] + d['daily_profit']
            recent_a_week[0] = add_when_not_zero(recent_a_week,d['recent_a_week'])
            recent_one_month_profit[0] = add_when_not_zero(recent_one_month_profit, d['recent_one_month_profit'])
            recent_three_month_profit[0] = add_when_not_zero(recent_three_month_profit, d['recent_three_month_profit'])
            recent_half_a_year_profit[0] = add_when_not_zero(recent_half_a_year_profit, d['recent_half_a_year_profit'])
            recent_a_year_profit[0] = add_when_not_zero(recent_a_year_profit, d['recent_a_year_profit'])

        tmp = [row['id'], row['name'], safe_div(avg_daily_income), avg_daily_income[1], safe_div(recent_a_week),
               safe_div(recent_one_month_profit), safe_div(recent_three_month_profit),
               safe_div(recent_half_a_year_profit), safe_div(recent_a_year_profit)]
        result.append(tmp)
        logging.info("current %d %s %f", index, tmp[1], tmp[2])


finally:
    df = pd.DataFrame(result, columns=['id', 'name', 'avg_daily_income', 'valid_fund_num', 'recent_a_week',
                                       'recent_one_month_profit', 'recent_three_month_profit',
                                       'recent_half_a_year_profit', 'recent_a_year_profit'])
    df = df.sort_values(by='avg_daily_income', ascending=False)
    df.to_csv('../data/' + time.strftime("avg_daily_income_%Y-%m-%d-%H-%M-%S", time.localtime()) + '.csv')
