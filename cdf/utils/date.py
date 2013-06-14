from datetime import datetime, timedelta


def date_2k_mn_to_date(nb_minutes):
    return datetime(2000, 1, 1) + timedelta(minutes=nb_minutes)
