import datetime


def get_days_from_now(ms_timestamp):
    now = datetime.datetime.now()
    delta = now - datetime.datetime.fromtimestamp(int(ms_timestamp) / 1000)
    return delta.days



