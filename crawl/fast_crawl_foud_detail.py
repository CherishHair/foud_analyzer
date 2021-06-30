import json
import logging
import os
import re
import threading
import time
from queue import Queue

import js2py
import pandas
import pandas as pd
import requests
from bs4 import BeautifulSoup

# 互斥锁实例化
lock = threading.Lock()

database = dict()
with open('../data/parsed_fund_detail.json', 'r') as f:
    database = json.load(f)


def get_data_by_code(code):
    try:
        res = database[str(code)]
    except KeyError:
        res = None
        pass
    if res is None:
        logging.info('正在从网络获取基金数据' + str(code))
        _, foud_type, foud_size, start_date = get_desc_by_code(code)
        url = 'http://fund.eastmoney.com/pingzhongdata/' + str(code) + '.js?v=' + time.strftime("%Y%m%d%H%M%S",
                                                                                                time.localtime())

        r = requests.get(url)
        if r.status_code != 200:
            logging.error("request failed,status_code=%d", r.status_code)
            return
        if r.text == "":
            logging.error("empty content,text=%d", r.text)
            return
        content = r.text
        context = js2py.EvalJs()
        context.execute(content)
        is_hb = context.ishb
        seven_days_year_income = None
        worth_trend = None
        stock_codes = None
        if is_hb:
            tmp_seven_days_year_income = []
            for item in context.Data_sevenDaysYearIncome:
                tmp_seven_days_year_income.append(item)
            seven_days_year_income = tmp_seven_days_year_income
        else:
            tmp_stock_codes = []
            for stock_code in context.stockCodes:
                tmp_stock_codes.append(stock_code)
            tmp_worth_trend = []
            for item in context.Data_ACWorthTrend:
                tmp_worth_trend.append(item)
            stock_codes = tmp_stock_codes
            worth_trend = tmp_worth_trend
        fund_name = context.fS_name
        fund_mgr_id = []
        for mgr in context.Data_currentFundManager:
            fund_mgr_id.append(mgr['id'])

        res = {
            "code": code,
            "name": fund_name,
            "fund_mgr_id": fund_mgr_id,
            "fund_type": foud_type,
            "is_hb": is_hb,
            "fund_size": foud_size,
            "start_date": start_date,
            "seven_days_year_income": seven_days_year_income,
            "stock_codes": stock_codes,
            "worth_trend": worth_trend
        }
    return res


def get_desc_by_code(code):
    code = str(code)
    url = 'http://fund.eastmoney.com/' + str(code) + '.html'
    logging.info('正在获取基金描述' + str(code))
    r = requests.get(url)
    r.encoding = 'utf-8'
    if r.status_code != 200:
        logging.error("request failed,status_code=%d", r.status_code)
        exit(-1)
    if r.text == "":
        logging.error("empty content,text=%d", r.text)
        exit(-1)
    content = r.text
    soup = BeautifulSoup(content, "html.parser")
    info = soup.find('div', class_='infoOfFund')
    table = info.table
    res = table.find_all('td')
    fund_type = res[0].a.text
    pattern = re.compile(r'\d+\.?\d*(?=亿元)')
    str_fs = pattern.findall(res[1].text)
    fund_size = '0.0'
    if len(str_fs) > 0:
        fund_size = str_fs[0]
    start_date = res[3].text[6:]
    return [code, fund_type, float(fund_size), start_date]


def fetch_data_by_code(task, data):
    context = js2py.EvalJs()
    while not task.empty():
        logging.info("task size=%d", task.qsize())
        code = task.get(False)
        if not isinstance(code, str) and not isinstance(code, int):
            print(code)
            continue

        logging.info('正在分析基金' + str(code))
        _, foud_type, foud_size, start_date = get_desc_by_code(code)
        url = 'http://fund.eastmoney.com/pingzhongdata/' + str(code) + '.js?v=' + time.strftime("%Y%m%d%H%M%S",
                                                                                                time.localtime())

        r = requests.get(url)
        if r.status_code != 200:
            logging.error("request failed,status_code=%d", r.status_code)
            return
        if r.text == "":
            logging.error("empty content,text=%d", r.text)
            return
        content = r.text
        context.execute(content)
        is_hb = context.ishb
        seven_days_year_income = None
        worth_trend = None
        stock_codes = None
        if is_hb:
            tmp_seven_days_year_income = []
            for item in context.Data_sevenDaysYearIncome:
                tmp_seven_days_year_income.append(item)
            seven_days_year_income = tmp_seven_days_year_income
        else:
            tmp_stock_codes = []
            for stock_code in context.stockCodes:
                tmp_stock_codes.append(stock_code)
            tmp_worth_trend = []
            for item in context.Data_ACWorthTrend:
                tmp_worth_trend.append(item)
            stock_codes = tmp_stock_codes
            worth_trend = tmp_worth_trend
        fund_name = context.fS_name
        fund_mgr_id = []
        for mgr in context.Data_currentFundManager:
            fund_mgr_id.append(mgr['id'])

        dic = {
            "code": code,
            "name": fund_name,
            "fund_mgr_id": fund_mgr_id,
            "fund_type": foud_type,
            "is_hb": is_hb,
            "fund_size": foud_size,
            "start_date": start_date,
            "seven_days_year_income": seven_days_year_income,
            "stock_codes": stock_codes,
            "worth_trend": worth_trend
        }
        lock.acquire()
        data[str(code)] = dic
        lock.release()


def build_database():
    task = Queue(0)
    code_lst = []
    lst = pd.read_csv('../data/2021-06-16.csv')
    for index, row in lst.iterrows():
        fouds_id = row['at_present_fouds_id']
        codes = re.findall(r"[0-9]+", fouds_id)
        for code in codes:
            code_lst.append(code)
    code_lst = list(set(code_lst))
    for code in code_lst:
        task.put(code)

    threads = []
    data = dict()
    for i in range(16):
        t = threading.Thread(target=fetch_data_by_code, args=(task, data))
        threads.append(t)
        t.start()
    # 等待线程结束
    for t in threads:
        t.join()
    f = open('../data/parsed_fund_detail.json', 'w')
    f.write(json.dumps(data))
    f.close()


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

print(get_data_by_code('010639'))

# build_database()
