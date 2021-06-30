import json
import logging
import math
import re
import time
import js2py
import requests
import utils.timestamp


database = dict()


# with open('./data/foud_datail.json', 'r') as f:
#     database = json.load(f)




def get_detail_by_code(code):
    try:
        content = database[str(code)]
    except Exception as e:
        content = ''
    if len(content) == 0:
        url = 'http://fund.eastmoney.com/pingzhongdata/' + str(code) + '.js?v=' + time.strftime("%Y%m%d%H%M%S",
                                                                                                time.localtime())
        logging.info('正在分析基金' + str(code))
        r = requests.get(url)
        if r.status_code != 200:
            logging.error("request failed,status_code=%d", r.status_code)
            exit(-1)
        if r.text == "":
            logging.error("empty content,text=%d", r.text)
            exit(-1)
        content = r.text
    return content


def get_data_by_code(code):
    content = get_detail_by_code(code)
    context = js2py.EvalJs()
    context.execute(content)
    is_hb = context.ishb
    if is_hb:
        logging.info('基金' + str(code) + '是货币基金')
        return [], True, False
    trend = context.Data_ACWorthTrend
    sz = trend.length
    if sz < 2:
        logging.info('基金' + str(code) + ' sz < 2')
        return [code, context.fS_name, "", int(time.time() * 1000), 0.0, 0.0], False, True
    if trend[0][1] is None or trend[sz - 1][1] is None or trend[0][0] is None:
        logging.info('基金' + str(code) + ' is None')
        return [code, context.fS_name, "", int(time.time() * 1000), 0.0, 0.0], False, True
    establish_date = trend[0][0]
    total_income = (trend[sz - 1][1] - trend[0][1]) / trend[0][1]
    stock_codes = context.stockCodes
    daily_income = float(total_income) / float(utils.timestamp.get_days_from_now(establish_date))
    col = [code, context.fS_name, stock_codes, establish_date, total_income, daily_income]
    return col, False, False


def calc_info_by_code(code):
    content = get_detail_by_code(code)
    context = js2py.EvalJs()
    context.execute(content)
    is_hb = context.ishb
    if is_hb:
        logging.info('基金' + str(code) + '是货币基金')
        return [], True, False
    trend = context.Data_ACWorthTrend
    sz = trend.length
    if sz < 2:
        logging.info('基金' + str(code) + ' sz < 2')
        return [code, context.fS_name, "", int(time.time() * 1000), 0.0, 0.0], False, True
    if trend[0][1] is None or trend[sz - 1][1] is None or trend[0][0] is None:
        logging.info('基金' + str(code) + ' is None')
        return [code, context.fS_name, "", int(time.time() * 1000), 0.0, 0.0], False, True
    establish_date = trend[0][0]
    total_profit = (trend[sz - 1][1] - trend[0][1]) / trend[0][1]
    stock_codes = context.stockCodes
    daily_profit = float(total_profit) / float(utils.timestamp.get_days_from_now(establish_date))
    recent_a_year_profit, _ = calc_recently_profit(trend, 365)
    recent_half_a_year_profit, _ = calc_recently_profit(trend, 365 // 2)
    recent_one_month_profit, _ = calc_recently_profit(trend, 30)
    recent_a_week, _ = calc_recently_profit(trend, 7)
    recent_three_month_profit, _ = calc_recently_profit(trend, 90)
    col = [code, context.fS_name, stock_codes, establish_date, total_profit, daily_profit, recent_a_week,
           recent_one_month_profit, recent_three_month_profit, recent_half_a_year_profit, recent_a_year_profit]
    return col, False, False


def calc_recently_profit(trend, days):
    sz = trend.length
    recent_date = int(utils.timestamp.get_timestamp_before_now(days) * 1000)
    pos = binary_search(trend, recent_date, 0, sz - 1)
    from_date = trend[pos][0]
    delta_days = utils.timestamp.get_days_from_now(from_date)
    if abs(delta_days - days) > (days / 4):
        return 0.0, True

    profit = (trend[sz - 1][1] - trend[pos][1]) / trend[pos][1]
    avg_profit = float(profit) / float(delta_days)
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


# print(calc_info_by_code('000513'))

