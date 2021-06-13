import logging
import time

import js2py
import requests

import utils.timestamp


def get_data_by_code(code):
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
    context = js2py.EvalJs()
    context.execute(r.text)
    isHb = context.ishb
    if isHb:
        logging.info('基金' + str(code)+'是货币基金')
        return [], True
    trend = context.Data_netWorthTrend
    sz = trend.length
    if sz == 0:
        return [code, context.fS_name, "", int(time.time() * 1000), 0.0, 0.0], False

    establish_date = trend[0].x
    total_income = (trend[sz - 1].y - trend[0].y) / trend[0].y
    stock_codes = context.stockCodes
    daily_income = float(total_income) / float(utils.timestamp.get_days_from_now(establish_date))
    col = [code, context.fS_name, stock_codes, establish_date, total_income, daily_income]
    return col, False
