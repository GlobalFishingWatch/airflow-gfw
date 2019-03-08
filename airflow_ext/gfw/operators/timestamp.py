from datetime import datetime, timedelta

AIRFLOW_DATE = "%Y-%m-%d"


def str2date(s):
    """
    :param s date in string expresed with the format AIRFLOW_DATE
    :type s string
    :return datetime representing the string s.
    """
    return datetime.strptime(s, AIRFLOW_DATE)


def daterange(start_date, end_date):
    """
    :param start_date start date
    :type start_date datetime
    :param end_date end date
    :type end_date datetime
    :return generator dates between start_date and end_date
    """
    for n in range(int((end_date - start_date).days)+1):
        yield start_date + timedelta(n)
