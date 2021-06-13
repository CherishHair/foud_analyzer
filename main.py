import logging
import re
import time

import pandas as pd
from crawl.crawl_foud_detail import get_data_by_code
from crawl.crawl_foud_mgr import get_all

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
get_data_by_code("000539")
data = pd.read_csv('./data/2021-06-13-14-38-23.csv')
result = []

try:
    for index, row in data.iterrows():
        fouds_id = row['at_present_fouds_id']
        codes = re.findall(r"[0-9]+", fouds_id)
        sz = len(codes)
        avg_daily_income = 0.0
        for code in codes:
            income, isHb = get_data_by_code(code)
            if isHb:
                sz = sz - 1
                continue
            daily_income = income[-1]
            avg_daily_income = avg_daily_income + daily_income
        if sz == 0:
            continue
        tmp = [row['id'], row['name'], avg_daily_income / sz]
        result.append(tmp)
        print('current: ' + str(index) + str(tmp))
except Exception as e:
    logger.error(e)
finally:
    df = pd.DataFrame(result, columns=['id', 'name', 'avg_daily_income'])
    df.to_csv('./data/' + time.strftime("avg_daily_income_%Y-%m-%d-%H-%M-%S", time.localtime()) + '.csv')
