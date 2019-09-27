from pymongo import MongoClient
import pandas as pd


client = MongoClient('10.0.1.19', username='root', password='BDqilingzhengfan1',)['red_book']


def f1():
    db1 = client['post_url']
    db2 = client['err_url']
    db3 = client['post_data']
    urls = set(pd.Series([i['originf_url'] for i in db3.find({}, {'originf_url': 1, '_id': 0})]).to_list())
    err_urls = set(pd.Series([i['_id'] for i in db1.find({}, {'_id': 1})]).to_list())
    urls = list(err_urls - urls)
    print(urls, len(urls))
    for url in urls:
        db2.save({'_id': str(url)})

if __name__ == '__main__':
    f1()
