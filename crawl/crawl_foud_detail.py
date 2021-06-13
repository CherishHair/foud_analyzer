import logging
import time

import js2py
import requests


def get_data_by_code(code):
    url = 'http://fund.eastmoney.com/pingzhongdata/' + str(code) + '.js?v=' + time.strftime("%Y%m%d%H%M%S",
                                                                                            time.localtime())
    r = requests.get(url)
    if r.status_code != 200:
        logging.error("request failed,status_code=%d", r.status_code)
        exit(-1)
    if r.text == "":
        logging.error("empty content,text=%d", r.text)
        exit(-1)
    context = js2py.EvalJs()
    context.execute(r.text)
    trend = context.Data_netWorthTrend
    sz = trend.length
    establish_date = trend[0].x
    total_income = (trend[sz-1].y-trend[0].y)/trend[0].y
    stock_codes = context.stockCodes
    col = [code,stock_codes,establish_date,total_income]
    return col


d = get_data_by_code("007685")
print(d)
