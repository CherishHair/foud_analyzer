import json
import logging
import re
import threading
import time
from queue import Queue
import pandas as pd
import requests

# 互斥锁实例化
lock = threading.Lock()


def get_data_by_code(task, data):
    while not task.empty():
        logging.info("task size=%d", task.qsize())
        code = task.get(False)
        url = 'http://fund.eastmoney.com/pingzhongdata/' + str(code) + '.js?v=' + time.strftime("%Y%m%d%H%M%S",
                                                                                                time.localtime())
        logging.info('正在分析基金' + str(code))
        r = requests.get(url)
        if r.status_code != 200:
            logging.error("request failed,status_code=%d", r.status_code)
            return
        if r.text == "":
            logging.error("empty content,text=%d", r.text)
            return
        lock.acquire()
        data[str(code)] = r.text
        lock.release()


def build_database():
    task = Queue(0)
    lst = pd.read_csv('../data/2021-06-13-14-38-23.csv')
    for index, row in lst.iterrows():
        fouds_id = row['at_present_fouds_id']
        codes = re.findall(r"[0-9]+", fouds_id)
        for code in codes:
            task.put(code)

    threads = []
    data = dict()
    for i in range(30):
        t = threading.Thread(target=get_data_by_code, args=(task, data))
        threads.append(t)
        t.start()
    # 等待线程结束
    for t in threads:
        t.join()
    f = open('../data/foud_datail.json', 'w')
    f.write(json.dumps(data))
    f.close()


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
build_database()
