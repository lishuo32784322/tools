from pymongo import MongoClient
import requests,re,time,random
import multiprocessing


class Proxy:
    client = MongoClient('10.0.1.19', username='root', password='BDqilingzhengfan1',)
    db = client.ips
    collection = db.ips

    def get_ip_from_net(self):
        url = 'http://lishuo3278.v4.dailiyun.com/query.txt?key=NP7594162C&word=&count=100&rand=false&detail=false'
        res = requests.get(url).text.split('\r')
        ips = ['http://' + i.strip() for i in res][:-1]
        print('获取的IP：', ips)
        for ip in ips:
            if self.verify_ip(ip) == 'yes':
                try:
                    if not self.collection.find_one({'ip': ip}):
                        self.collection.insert({'ip':ip})
                    print('插入一条新IP')
                except Exception as e :
                    print('err',e)


    def verify_ip(self,ip):
        url = 'http://www.httpbin.org/ip'
        a, b = ip.split('://')
        proxies = {a:b}
        try:
            html = requests.get(url, proxies= proxies, timeout = 3)
            if '120.131.10.99' not in  html:return 'yes'
            else:return 'no'
        except:return 'no'

    def verify_db_num(self):
        while 1:
            try:
                # print(11)
                self.get_ip_from_net()
                time.sleep(3)
            except:
                pass


    def verify_db(self):
        while 1:
            try:
                ips = self.collection.find()
                ips = [i['ip'] for i in ips]
                print('验证IP列表', ips)
                for ip in ips:
                    if self.verify_ip(ip) == 'no':
                        self.collection.remove({'ip': ip})
                        print('删除不可用IP：',ip)
            except:pass




if __name__ == '__main__':
    proxy = Proxy()
    work1 = proxy.verify_db_num
    work2 = proxy.verify_db
    work3 = proxy.verify_db
    work1 = multiprocessing.Process(name='work1',target=work1)
    work2 = multiprocessing.Process(name='work2',target=work2)
    work3 = multiprocessing.Process(name='work2',target=work3)
    work1.start()
    work2.start()
    work3.start()


