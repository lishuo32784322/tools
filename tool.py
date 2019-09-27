def format_cookies(s):
    dic = {}
    s = s.split(':', maxsplit = 1)[1]
    s = s.split(';')
    for i in s :
        key,value = i.split('=', maxsplit=1)
        dic[key] = value
    return dic


def format_string(s):
    # print(s)
    import re
    dic = {}
    s = re.search('-H(.*?)--compressed',s).group(1)
    s = s.split('-H')

    for i in s[:-1]:
        key,value = i.split(':',maxsplit=1)
        key, value = key.replace('\'','').strip(), value.replace('\'','').strip()
        dic[key] = value
    return dic



if __name__ == '__main__':

    # s = 'cookie: _zap=e8253a03-c231-48e1-8e03-f6fbe1d1a8fa; _xsrf=WzqLvDunr3gAozFS3rk13Dc7K9E7XX1D; d_c0="AIBnbuLEbw-PTkMji46PGSnEksfXpGQmBME=|1557978733"; capsion_ticket="2|1:0|10:1557978749|14:capsion_ticket|44:Yjk0ZTI1YjBjNzBiNGI2NDllZDcyYjkyYWJkZGMzYzM=|5ab1ab074ff6184e67596550fd9c26b166435f61d50bde1ec92b2d8a67d3cd81"; z_c0="2|1:0|10:1557978755|4:z_c0|92:Mi4xTUNDUkNRQUFBQUFBZ0dkdTRzUnZEeVlBQUFCZ0FsVk5neXpLWFFETTltZ3FSQ2pzdVdIbng2YXJxeGE5czk3a253|2530e48d90b97711ef4c48a4c8c07d8ecf718a9059931b5a39b0445f18681f73"; tst=r; q_c1=47d2af97157642a8b5745da263c15b19|1557978757000|1557978757000; tgw_l7_route=66cb16bc7f45da64562a077714739c11; __utma=51854390.1389669611.1557979770.1557979770.1557979770.1; __utmb=51854390.0.10.1557979770; __utmc=51854390; __utmz=51854390.1557979770.1.1.utmcsr=zhihu.com|utmccn=(referral)|utmcmd=referral|utmcct=/; __utmv=51854390.100--|2=registration_date=20180521=1^3=entry_date=20180521=1'
    # print(format_cookies(s))

    # s = "curl -H 'Host: www.xiaohongshu.com' -H 'Accept: */*' -H 'Auth-Sign: d7c031d227040268f8c6458302462d7c' -H 'Authorization: 57d71d6a-40a2-40c8-870b-b3cacf8f3bdf' -H 'Auth: eyJoYXNoIjoibWQ0IiwiYWxnIjoiSFMyNTYiLCJ0eXAiOiJKV1QifQ.eyJzaWQiOiIxMjk3MzlhZjRjNjc0ZDQzY2RkMGRmYjFlYjBjNDEwYyIsImV4cGlyZSI6MTU1ODU4NTMwOX0.U81JQZ98_695L0owgkXXoF3k96YsEUYgau01CNiOkNA' -H 'Accept-Language: zh-cn' -H 'Device-Fingerprint: WHJMrwNw1k/HU+r3oYFpgKlivD2fSujqTVQNNFvHVgjo++OSCZB3fXQGwAUZCu0TQH8BoZvBelzs7KcnOjjaGchiWE4Jlef7wdCW1tldyDzmauSxIJm5Txg==1487582755342' -H 'Content-Type: application/json' -H 'Referer: https://servicewechat.com/wxffc08ac7df482a27/229/page-frame.html' -H 'User-Agent: Mozilla/5.0 (iPhone; CPU iPhone OS 10_3_3 like Mac OS X) AppleWebKit/603.3.8 (KHTML, like Gecko) Mobile/14G60 MicroMessenger/7.0.4(0x17000428) NetType/WIFI Language/zh_CN' --compressed 'https://www.xiaohongshu.com/wx_mp_api/sns/v1/user/57f4954250c4b43670a73ca8/info?sid=session.1558582579592269310895'"
    # format_string(s)








    pass