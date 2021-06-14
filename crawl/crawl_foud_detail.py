import json
import logging
import time
import js2py
import requests
import utils.timestamp

database = dict()
with open('./data/foud_datail.json', 'r') as f:
    database = json.load(f)

def get_data_by_code(code):
    content = database[str(code)]
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
