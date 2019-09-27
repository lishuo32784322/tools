import pandas as pd
from pymongo import MongoClient


client = MongoClient('120.92.49.240', username='root', password='BDqilingzhengfan1')
data_list = []

for i in client.all_comms.new_comms.find({}, {'_id': 0, 'comms_like': 1}):
    if i['comms_like'] != 0:
        data_list.append(i['comms_like'])


df = pd.Series(data_list)
df = df.values.tolist()

point = len(df) // 100
df = sorted(df)

df_list = []
for i in range(1,101):
    df_list.append(pd.Series([f'{i}%', df[point * (i)]]))
    print(df[point * (i)])
df_list = pd.DataFrame(df_list)
print(max(df), len(df))
print(df_list)

df_list.to_excel('/data/workspace/tools/comms_likes.xlsx', index=False)
