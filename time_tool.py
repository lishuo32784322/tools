from datetime import datetime, date, timedelta
import re,time


def parse_time(data):
    a = data
    # print(date)
    if re.match('刚刚', data):
        a = time.strftime('%Y-%m-%d %H:%M', time.localtime(time.time()))
    if re.match('\d+分钟前', data):
        minute = re.match('(\d+)', data).group(1)
        a = time.strftime('%Y-%m-%d %H:%M', time.localtime(time.time() - float(minute) * 60))
    if re.match('\d+小时前', data):
        hour = re.match('(\d+)', data).group(1)
        a = time.strftime('%Y-%m-%d %H:%M', time.localtime(time.time() - float(hour) * 60 * 60))
    if re.match('昨天.*', data):
        yesterday = (date.today() + timedelta(days=-1)).strftime("%Y-%m-%d")
        a = re.match('昨天(.*)', data).group(1).strip()
        a = yesterday + ' ' + a
    if re.match('\d{2}-\d{2}', data):
        a = data
    return a


def parse_time1(dd):
    GMT_FORMAT = '%a %b %d %Y %H:%M:%S GMT+0800 (CST)'
    return datetime.strptime(dd, GMT_FORMAT)

# parse_time1("Fri Nov 09 2018 14:41:35 GMT+0800 (CST)")