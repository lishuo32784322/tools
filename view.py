import pandas as pd
from pymongo import MongoClient
import gevent.monkey
gevent.monkey.patch_all()
from gevent.pool import Pool
client = MongoClient('120.92.49.240', username='root', password='BDqilingzhengfan1')


data_list = []


def count(data):
    data_list.append(data['comms_like'])


pool = Pool(500)
num = 0
while num < 7100000:
    pool.map(count, client.all_comms.comms.find({}, {'_id': 0, 'comms_like': 1}).skip(num).limit(1000000))
    num = num + 1000000
    print(num, len(data_list))
# pool.map(change, client.all_post.post_data.find())

df = pd.Series(data_list)
df = df.sort_values().tolist()
point = len(df)//100
print(point)
datas = []
for i in range(100):
    print(df[point * i])
    datas.append(pd.Series([f'{i+1}%', df[point * i]]))

df = pd.DataFrame(datas)
df = df.to_excel('/data/workspace/tools/comms_likes.xlsx', index=False)
