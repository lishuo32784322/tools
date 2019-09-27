import requests


def verify_ip(ip):
    # print(ip)
    url = 'http://www.httpbin.org/ip'
    a, b = ip.split('://')
    proxies = {a: b}
    # print(proxies)
    try:
        html = requests.get(url, proxies=proxies, timeout=1)
        if requests.get(url) != html:
            return 'yes'
        else:
            return 'no'
    except:
        return 'no'