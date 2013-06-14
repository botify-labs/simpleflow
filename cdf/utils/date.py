from datetime import datetime, timedelta


def date_2k_mn_to_date(date_str):
    return datetime(2000, 1, 1) + timedelta(minutes=int(date_str))
