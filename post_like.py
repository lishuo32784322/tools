from pymongo import MongoClient
import gevent.monkey
gevent.monkey.patch_all()
from gevent.pool import Pool

client = MongoClient('120.92.49.240', username='root', password='BDqilingzhengfan1')


def change(i):
    try:
        i['likes']
    except:
        client.all_post.post_data.update({'_id': i['_id']}, {'$set': {'likes': 0}})
    try:
        i['share']
    except:
        client.all_post.post_data.update({'_id': i['_id']}, {'$set': {'share': 0}})
    try:
        i['favorite']
    except:
        client.all_post.post_data.update({'_id': i['_id']}, {'$set': {'favorite': 0}})


pool = Pool(100)
pool.map(change, client.all_post.post_data.find())
