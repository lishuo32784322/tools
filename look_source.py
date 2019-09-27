import pandas as pd
from pymongo import MongoClient


client = MongoClient('120.92.49.240', username='root', password='BDqilingzhengfan1')
dic = {}

for i in client.all_post.post_data.find({}, {'_id': 0, 'likes': 1, 'favorite': 1, 'origin_user_id': 1}):
    if i['origin_user_id'] in dic.keys():
        dic[i['origin_user_id']] = dic[i['origin_user_id']] + int(i['likes'])+int(i['favorite'])
    else:
        dic[i['origin_user_id']] = int(i['likes'])+int(i['favorite'])


df = pd.Series(dic)
df = df.values.tolist()
df = sorted(df)

point = len(df) // 100

df_list = []
for i in range(100):
    df_list.append(pd.Series([f'{i+1}%', df[point * (i)]]))
df_list = pd.DataFrame(df_list)
print(df, max(df), len(df))
print(df_list)

df_list.to_excel('/data/workspace/tools/user_likes+favorite.xlsx', index=False)
