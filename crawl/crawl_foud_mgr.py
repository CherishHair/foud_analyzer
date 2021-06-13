import time

import requests
import logging
import re
import js2py
import pandas as pd


def get_data_by_page(page=1):
    url = 'http://fund.eastmoney.com/Data/FundDataPortfolio_Interface.aspx?dt=14&mc=returnjson&ft=all&pn=50&sc=abbname' \
          '&st=asc&pi=' + str(page)
    r = requests.get(url)
    if r.status_code != 200:
        logging.error("request failed,status_code=%d", r.status_code)
        exit(-1)
    if r.text == "":
        logging.error("empty content,text=%d", r.text)
        exit(-1)
    jsObj = js2py.eval_js(r.text)

    data = []
    for d in jsObj.data:
        result = ["-1.0"]
        if d[-2] != "--":
            result = re.findall(r"""[0-9]+\.[0-9]*""", d[-2])
            if (d[-2])[-2:] != '亿元':
                logging.error("invalid foud size = ", d[-2])
        col = [d[0], d[1], d[2], d[3], d[4], d[5], int(d[6]), float(result[0]) * 100000000]
        data.append(col)
    return data


def get_all():
    page = 1
    all_data = []
    while True:
        data = get_data_by_page(page)
        if len(data) > 0:
            page = page + 1
            all_data.extend(data)
            # time.sleep(1)
        else:
            break
        df = pd.DataFrame(all_data,
                          columns=['id', 'name', 'corp_id', 'corp_name', 'at_present_fouds_id', 'at_present_fouds_name',
                                   'job_duration_days', 'at_present_total_size'])
        df.to_csv('../data/' + time.strftime("%Y-%m-%d-%H-%M-%S", time.localtime()) + '.csv')
        return df



