import datetime
import time
import csv

from opsgenie.swagger_client import AlertApi
from opsgenie.swagger_client import configuration

DEFAULT_API_KEY = 'XXXXXXXXXXXX'
API_AUTHORIZATION_KEY = 'Authorization'


def main():
    alert_lst = [[]]
    client = get_swagger_client()
    create_alert_lst(client, 1546300800, alert_lst)


def get_swagger_client():
    """
    This method connecting to Akamai-opsgenie with the Default API key
    :return: (object) swagger client of the opsgenie SDK
    """
    configuration.api_key[API_AUTHORIZATION_KEY] = DEFAULT_API_KEY
    configuration.api_key_prefix[API_AUTHORIZATION_KEY] = 'GenieKey'
    opsgenie_client = AlertApi()
    return opsgenie_client


def create_alert_lst(opsgenie_client, date, alerts_lst):
    try:
        for i in xrange(200):
            alerts = opsgenie_client.list_alerts(offset=i * 100, limit=100, query='createdAt >= {}'.format(date)).data
            for alert in alerts:
              alerts_lst.append([alert.message, round_to_hour_epoch(alert.created_at)])
    finally:
        write_alerts_to_file(alerts_lst)


def round_to_hour_epoch(dt):
    dt_start_of_hour = dt.replace(minute=0, second=0, microsecond=0)
    dt_half_hour = dt.replace(minute=30, second=0, microsecond=0)
    if dt >= dt_half_hour:
        dt = dt_start_of_hour + datetime.timedelta(hours=1)
    else:
        dt = dt_start_of_hour

    return int(time.mktime(dt.timetuple()))


def write_alerts_to_file(alerts_lst):
    with open('alerts.csv', 'w') as writeFile:
        writer = csv.writer(writeFile)
        writer.writerows(alerts_lst)


main()
