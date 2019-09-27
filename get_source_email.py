import pandas as pd
from sqlalchemy import create_engine
import gevent.monkey
gevent.monkey.patch_all()
from gevent.pool import Pool
import smtplib
from email.mime.text import MIMEText


engine = create_engine('mysql+pymysql://admin:BDqilingzhengfan1@10.0.1.20:3306/bd_operation')
engine1 = create_engine('mysql+pymysql://admin:BDqilingzhengfan1@10.0.1.21:3306/bd_recommend')


def f(rec_status):
    get_post_sql = f'select post_id from media_rec where {rec_status} and del_status = 0'
    ids = pd.read_sql(get_post_sql, con=engine1)
    ids = tuple(set(sum(ids.values.tolist(), [])))

    get_tags_sql = f'select post_id, tag_id from post_tag where post_id in {ids}'
    ids_tags = pd.read_sql(get_tags_sql, con=engine)
    ids_tags.drop_duplicates(keep='first', inplace=True)

    post_ids = tuple(ids_tags['post_id'].values.tolist())
    tag_level_sql = f'select post_id, content_cat_level from post_attribute where post_id in {post_ids}'
    tag_level = pd.read_sql(tag_level_sql, con=engine)
    tag_level.drop_duplicates(keep='first', inplace=True)

    result = pd.merge(ids_tags, tag_level, how='outer', on='post_id')  # 合并两个dataframe
    result = result[result.content_cat_level != 0]
    result.drop_duplicates(subset=['post_id', 'tag_id'], keep='first', inplace=True)
    result.dropna(axis=0, how='any', inplace=True)

    tag_ids = tuple(result['tag_id'].values.tolist())
    tag_name_sql = f'select id, name from tag_info where id in {tag_ids}'
    tag_name = pd.read_sql(tag_name_sql, con=engine)
    tag_name.rename(columns={'id': 'tag_id', 'name': 'tag_name'}, inplace=True)

    result = pd.merge(result, tag_name, how='outer', on='tag_id')
    result.drop_duplicates(subset=['post_id', 'tag_id'], keep='first', inplace=True)

    one_count = \
    result[result.content_cat_level == 30].drop_duplicates(subset=['post_id'], keep='first', inplace=False).shape[0]
    two_count = \
    result[result.content_cat_level == 60].drop_duplicates(subset=['post_id'], keep='first', inplace=False).shape[0]
    three_count = \
    result[result.content_cat_level == 90].drop_duplicates(subset=['post_id'], keep='first', inplace=False).shape[0]
    all_count = one_count + two_count + three_count

    one_tag = result[result.content_cat_level == 30]
    one_tag.drop_duplicates(subset=['post_id'], keep='first', inplace=True)
    one_tag = one_tag['tag_name'].value_counts()[:20]
    one_index = list(one_tag.index)
    one_value = one_tag.values.tolist()

    two_tag = result[result.content_cat_level == 60]
    two_tag.drop_duplicates(subset=['post_id'], keep='first', inplace=True)
    two_tag = two_tag['tag_name'].value_counts()[:20]
    two_index = list(two_tag.index)
    two_value = two_tag.values.tolist()

    three_tag = result[result.content_cat_level == 90]
    three_tag.drop_duplicates(subset=['post_id'], keep='first', inplace=True)
    three_tag = three_tag['tag_name'].value_counts()[:20]
    three_index = list(three_tag.index)
    three_value = three_tag.values.tolist()

    if rec_status == 'rec_status = -4':
        name = '冷启'
    elif rec_status == 'rec_status = -2':
        name = '推荐'
    else:
        name = '推荐+冷启'

    one_table = [one_count, all_count, str(round(one_count / all_count, 4) * 100)[:4] + '%', name]
    two_table = [two_count, all_count, str(round(two_count / all_count, 4) * 100)[:4] + '%', name]
    three_table = [three_count, all_count, str(round(three_count / all_count, 4) * 100)[:4] + '%', name]

    text = f'''
    <table border="1" width="100%" bgcolor="#e9faff">
        <tr align="center">
            <td colspan="3">内容名称</td>
            <td>数量</td>
            <td>总数量</td>
            <td>占比</td>
            <td>库名</td>
        </tr>

        <tr align="center">
            <td colspan="3">一级内容标签</td>
            <td>{one_table[0]}</td>
            <td>{one_table[1]}</td>
            <td>{one_table[2]}</td>
            <td>{one_table[3]}</td>
        </tr>

        <tr align="center">
             <td colspan="3">二级内容标签</td>
            <td>{two_table[0]}</td>
            <td>{two_table[1]}</td>
            <td>{two_table[2]}</td>
            <td>{two_table[3]}</td>

        </tr>

        <tr align="center">
            <td colspan="3">三级内容标签</td>
            <td>{three_table[0]}</td>
            <td>{three_table[1]}</td>
            <td>{three_table[2]}</td>
            <td>{three_table[3]}</td>

        </tr>
        <tr align="center">
            <td rowspan="21" style="text-align: center;"><span style="width:30%;display: block;margin: 0 auto">一级标签</span></td>
            <td>标签名称</td>
            <td>贴子数量</td>
            <td>总数量</td>
            <td>占比</td>
            <td>当前库总数量</td>
            <td>占比</td>

        </tr>
        <tr align="center">

            <td>{one_index[0]}</td>
            <td>{one_value[0]}</td>
            <td>{one_count}</td>
            <td>{str(round(one_value[0] / one_count, 4) * 100)[:4] + '%'}</td>
            <td>{all_count}</td>
            <td>{str(round(one_value[0] / all_count, 4) * 100)[:4] + '%'}</td>
        </tr>
        <tr align="center">

            <td>{one_index[1]}</td>
            <td>{one_value[1]}</td>
            <td>{one_count}</td>
            <td>{str(round(one_value[1] / one_count, 4) * 100)[:4] + '%'}</td>
            <td>{all_count}</td>
            <td>{str(round(one_value[1] / all_count, 4) * 100)[:4] + '%'}</td>
        </tr>
        <tr align="center">

            <td>{one_index[2]}</td>
            <td>{one_value[2]}</td>
            <td>{one_count}</td>
            <td>{str(round(one_value[2] / one_count, 4) * 100)[:4] + '%'}</td>
            <td>{all_count}</td>
            <td>{str(round(one_value[2] / all_count, 4) * 100)[:4] + '%'}</td>
        </tr>
        <tr align="center">

            <td>{one_index[3]}</td>
            <td>{one_value[3]}</td>
            <td>{one_count}</td>
            <td>{str(round(one_value[3] / one_count, 4) * 100)[:4] + '%'}</td>
            <td>{all_count}</td>
            <td>{str(round(one_value[3] / all_count, 4) * 100)[:4] + '%'}</td>
        </tr>
        <tr align="center">

            <td>{one_index[4]}</td>
            <td>{one_value[4]}</td>
            <td>{one_count}</td>
            <td>{str(round(one_value[4] / one_count, 4) * 100)[:4] + '%'}</td>
            <td>{all_count}</td>
            <td>{str(round(one_value[4] / all_count, 4) * 100)[:4] + '%'}</td>
        </tr>
        <tr align="center">

            <td>{one_index[5]}</td>
            <td>{one_value[5]}</td>
            <td>{one_count}</td>
            <td>{str(round(one_value[5] / one_count, 4) * 100)[:4] + '%'}</td>
            <td>{all_count}</td>
            <td>{str(round(one_value[5] / all_count, 4) * 100)[:4] + '%'}</td>
        </tr>
        <tr align="center">
            <td>{one_index[6]}</td>
            <td>{one_value[6]}</td>
            <td>{one_count}</td>
            <td>{str(round(one_value[6] / one_count, 4) * 100)[:4] + '%'}</td>
            <td>{all_count}</td>
            <td>{str(round(one_value[6] / all_count, 4) * 100)[:4] + '%'}</td>
        </tr>
        <tr align="center">

            <td>{one_index[7]}</td>
            <td>{one_value[7]}</td>
            <td>{one_count}</td>
            <td>{str(round(one_value[7] / one_count, 4) * 100)[:4] + '%'}</td>
            <td>{all_count}</td>
            <td>{str(round(one_value[7] / all_count, 4) * 100)[:4] + '%'}</td>
        </tr>
        <tr align="center">

            <td>{one_index[8]}</td>
            <td>{one_value[8]}</td>
            <td>{one_count}</td>
            <td>{str(round(one_value[8] / one_count, 4) * 100)[:4] + '%'}</td>
            <td>{all_count}</td>
            <td>{str(round(one_value[8] / all_count, 4) * 100)[:4] + '%'}</td>
        </tr>
        <tr align="center">

            <td>{one_index[9]}</td>
            <td>{one_value[9]}</td>
            <td>{one_count}</td>
            <td>{str(round(one_value[9] / one_count, 4) * 100)[:4] + '%'}</td>
            <td>{all_count}</td>
            <td>{str(round(one_value[9] / all_count, 4) * 100)[:4] + '%'}</td>
        </tr>
        <tr align="center">

            <td>{one_index[10]}</td>
            <td>{one_value[10]}</td>
            <td>{one_count}</td>
            <td>{str(round(one_value[10] / one_count, 4) * 100)[:4] + '%'}</td>
            <td>{all_count}</td>
            <td>{str(round(one_value[10] / all_count, 4) * 100)[:4] + '%'}</td>
        </tr>
        <tr align="center">
            <td>{one_index[11]}</td>
            <td>{one_value[11]}</td>
            <td>{one_count}</td>
            <td>{str(round(one_value[11] / one_count, 4) * 100)[:4] + '%'}</td>
            <td>{all_count}</td>
            <td>{str(round(one_value[11] / all_count, 4) * 100)[:4] + '%'}</td>
        </tr>
        <tr align="center">

            <td>{one_index[12]}</td>
            <td>{one_value[12]}</td>
            <td>{one_count}</td>
            <td>{str(round(one_value[12] / one_count, 4) * 100)[:4] + '%'}</td>
            <td>{all_count}</td>
            <td>{str(round(one_value[12] / all_count, 4) * 100)[:4] + '%'}</td>
        </tr>
        <tr align="center">
            <td>{one_index[13]}</td>
            <td>{one_value[13]}</td>
            <td>{one_count}</td>
            <td>{str(round(one_value[13] / one_count, 4) * 100)[:4] + '%'}</td>
            <td>{all_count}</td>
            <td>{str(round(one_value[13] / all_count, 4) * 100)[:4] + '%'}</td>
        </tr>
        <tr align="center">
            <td>{one_index[14]}</td>
            <td>{one_value[14]}</td>
            <td>{one_count}</td>
            <td>{str(round(one_value[14] / one_count, 4) * 100)[:4] + '%'}</td>
            <td>{all_count}</td>
            <td>{str(round(one_value[14] / all_count, 4) * 100)[:4] + '%'}</td>
        </tr>
        <tr align="center">
            <td>{one_index[15]}</td>
            <td>{one_value[15]}</td>
            <td>{one_count}</td>
            <td>{str(round(one_value[15] / one_count, 4) * 100)[:4] + '%'}</td>
            <td>{all_count}</td>
            <td>{str(round(one_value[15] / all_count, 4) * 100)[:4] + '%'}</td>
        </tr>
        <tr align="center">
            <td>{one_index[16]}</td>
            <td>{one_value[16]}</td>
            <td>{one_count}</td>
            <td>{str(round(one_value[16] / one_count, 4) * 100)[:4] + '%'}</td>
            <td>{all_count}</td>
            <td>{str(round(one_value[16] / all_count, 4) * 100)[:4] + '%'}</td>
        </tr>
        <tr align="center">
            <td>{one_index[17]}</td>
            <td>{one_value[17]}</td>
            <td>{one_count}</td>
            <td>{str(round(one_value[17] / one_count, 4) * 100)[:4] + '%'}</td>
            <td>{all_count}</td>
            <td>{str(round(one_value[17] / all_count, 4) * 100)[:4] + '%'}</td>
        </tr>
        <tr align="center">
            <td>{one_index[18]}</td>
            <td>{one_value[18]}</td>
            <td>{one_count}</td>
            <td>{str(round(one_value[18] / one_count, 4) * 100)[:4] + '%'}</td>
            <td>{all_count}</td>
            <td>{str(round(one_value[18] / all_count, 4) * 100)[:4] + '%'}</td>
        </tr>
        <tr align="center">
            <td>{one_index[19]}</td>
            <td>{one_value[19]}</td>
            <td>{one_count}</td>
            <td>{str(round(one_value[19] / one_count, 4) * 100)[:4] + '%'}</td>
            <td>{all_count}</td>
            <td>{str(round(one_value[19] / all_count, 4) * 100)[:4] + '%'}</td>
        </tr>
        <tr align="center">
            <td rowspan="21"><span style="width:30%;display: block;margin: 0 auto">二级标签</span></td>
            <td>标签名称</td>
            <td>贴子数量</td>
            <td>总数量</td>
            <td>占比</td>
            <td>当前库总数量</td>
            <td>占比</td>
        </tr>
         <tr align="center">
            <td>{two_index[0]}</td>
            <td>{two_value[0]}</td>
            <td>{two_count}</td>
            <td>{str(round(two_value[0] / two_count, 4) * 100)[:4] + '%'}</td>
            <td>{all_count}</td>
            <td>{str(round(two_value[0] / all_count, 4) * 100)[:4] + '%'}</td>
        </tr>
        <tr align="center">
            <td>{two_index[1]}</td>
            <td>{two_value[1]}</td>
            <td>{two_count}</td>
            <td>{str(round(two_value[1] / two_count, 4) * 100)[:4] + '%'}</td>
            <td>{all_count}</td>
            <td>{str(round(two_value[1] / all_count, 4) * 100)[:4] + '%'}</td>
        </tr>
        <tr align="center">
            <td>{two_index[2]}</td>
            <td>{two_value[2]}</td>
            <td>{two_count}</td>
            <td>{str(round(two_value[2] / two_count, 4) * 100)[:4] + '%'}</td>
            <td>{all_count}</td>
            <td>{str(round(two_value[2] / all_count, 4) * 100)[:4] + '%'}</td>
        </tr>
        <tr align="center">
            <td>{two_index[3]}</td>
            <td>{two_value[3]}</td>
            <td>{two_count}</td>
            <td>{str(round(two_value[3] / two_count, 4) * 100)[:4] + '%'}</td>
            <td>{all_count}</td>
            <td>{str(round(two_value[3] / all_count, 4) * 100)[:4] + '%'}</td>
        </tr>
        <tr align="center">
            <td>{two_index[4]}</td>
            <td>{two_value[4]}</td>
            <td>{two_count}</td>
            <td>{str(round(two_value[4] / two_count, 4) * 100)[:4] + '%'}</td>
            <td>{all_count}</td>
            <td>{str(round(two_value[4] / all_count, 4) * 100)[:4] + '%'}</td>
        </tr>
        <tr align="center">
            <td>{two_index[5]}</td>
            <td>{two_value[5]}</td>
            <td>{two_count}</td>
            <td>{str(round(two_value[5] / two_count, 4) * 100)[:4] + '%'}</td>
            <td>{all_count}</td>
            <td>{str(round(two_value[5] / all_count, 4) * 100)[:4] + '%'}</td>
        </tr>
        <tr align="center">
            <td>{two_index[6]}</td>
            <td>{two_value[6]}</td>
            <td>{two_count}</td>
            <td>{str(round(two_value[6] / two_count, 4) * 100)[:4] + '%'}</td>
            <td>{all_count}</td>
            <td>{str(round(two_value[6] / all_count, 4) * 100)[:4] + '%'}</td>
        </tr>
        <tr align="center">
            <td>{two_index[7]}</td>
            <td>{two_value[7]}</td>
            <td>{two_count}</td>
            <td>{str(round(two_value[7] / two_count, 4) * 100)[:4] + '%'}</td>
            <td>{all_count}</td>
            <td>{str(round(two_value[7] / all_count, 4) * 100)[:4] + '%'}</td>
        </tr>
        <tr align="center">
            <td>{two_index[8]}</td>
            <td>{two_value[8]}</td>
            <td>{two_count}</td>
            <td>{str(round(two_value[8] / two_count, 4) * 100)[:4] + '%'}</td>
            <td>{all_count}</td>
            <td>{str(round(two_value[8] / all_count, 4) * 100)[:4] + '%'}</td>
        </tr>
        <tr align="center">
            <td>{two_index[9]}</td>
            <td>{two_value[9]}</td>
            <td>{two_count}</td>
            <td>{str(round(two_value[9] / two_count, 4) * 100)[:4] + '%'}</td>
            <td>{all_count}</td>
            <td>{str(round(two_value[9] / all_count, 4) * 100)[:4] + '%'}</td>
        </tr>
        <tr align="center">
            <td>{two_index[10]}</td>
            <td>{two_value[10]}</td>
            <td>{two_count}</td>
            <td>{str(round(two_value[10] / two_count, 4) * 100)[:4] + '%'}</td>
            <td>{all_count}</td>
            <td>{str(round(two_value[10] / all_count, 4) * 100)[:4] + '%'}</td>
        </tr>
        <tr align="center">
            <td>{two_index[11]}</td>
            <td>{two_value[11]}</td>
            <td>{two_count}</td>
            <td>{str(round(two_value[11] / two_count, 4) * 100)[:4] + '%'}</td>
            <td>{all_count}</td>
            <td>{str(round(two_value[11] / all_count, 4) * 100)[:4] + '%'}</td>
        </tr>
        <tr align="center">
            <td>{two_index[12]}</td>
            <td>{two_value[12]}</td>
            <td>{two_count}</td>
            <td>{str(round(two_value[12] / two_count, 4) * 100)[:4] + '%'}</td>
            <td>{all_count}</td>
            <td>{str(round(two_value[12] / all_count, 4) * 100)[:4] + '%'}</td>
        </tr>
        <tr align="center">
            <td>{two_index[13]}</td>
            <td>{two_value[13]}</td>
            <td>{two_count}</td>
            <td>{str(round(two_value[13] / two_count, 4) * 100)[:4] + '%'}</td>
            <td>{all_count}</td>
            <td>{str(round(two_value[13] / all_count, 4) * 100)[:4] + '%'}</td>
        </tr>
        <tr align="center">
            <td>{two_index[14]}</td>
            <td>{two_value[14]}</td>
            <td>{two_count}</td>
            <td>{str(round(two_value[14] / two_count, 4) * 100)[:4] + '%'}</td>
            <td>{all_count}</td>
            <td>{str(round(two_value[14] / all_count, 4) * 100)[:4] + '%'}</td>
        </tr>
        <tr align="center">
            <td>{two_index[15]}</td>
            <td>{two_value[15]}</td>
            <td>{two_count}</td>
            <td>{str(round(two_value[15] / two_count, 4) * 100)[:4] + '%'}</td>
            <td>{all_count}</td>
            <td>{str(round(two_value[15] / all_count, 4) * 100)[:4] + '%'}</td>
        </tr>
        <tr align="center">
            <td>{two_index[16]}</td>
            <td>{two_value[16]}</td>
            <td>{two_count}</td>
            <td>{str(round(two_value[16] / two_count, 4) * 100)[:4] + '%'}</td>
            <td>{all_count}</td>
            <td>{str(round(two_value[16] / all_count, 4) * 100)[:4] + '%'}</td>
        </tr>
        <tr align="center">
            <td>{two_index[17]}</td>
            <td>{two_value[17]}</td>
            <td>{two_count}</td>
            <td>{str(round(two_value[17] / two_count, 4) * 100)[:4] + '%'}</td>
            <td>{all_count}</td>
            <td>{str(round(two_value[17] / all_count, 4) * 100)[:4] + '%'}</td>
        </tr>
        <tr align="center">
            <td>{two_index[18]}</td>
            <td>{two_value[18]}</td>
            <td>{two_count}</td>
            <td>{str(round(two_value[18] / two_count, 4) * 100)[:4] + '%'}</td>
            <td>{all_count}</td>
            <td>{str(round(two_value[18] / all_count, 4) * 100)[:4] + '%'}</td>
        </tr>
        <tr align="center">
            <td>{two_index[19]}</td>
            <td>{two_value[19]}</td>
            <td>{two_count}</td>
            <td>{str(round(two_value[19] / two_count, 4) * 100)[:4] + '%'}</td>
            <td>{all_count}</td>
            <td>{str(round(two_value[19] / all_count, 4) * 100)[:4] + '%'}</td>
        </tr>
        <tr align="center">
            <td rowspan="21"><span style="width:30%;display: block;margin: 0 auto">三级标签</span></td>
            <td>标签名称</td>
            <td>贴子数量</td>
            <td>总数量</td>
            <td>占比</td>
            <td>当前库总数量</td>
            <td>占比</td>
        </tr>
        <tr align="center">
            <td>{three_index[0]}</td>
            <td>{three_value[0]}</td>
            <td>{three_count}</td>
            <td>{str(round(three_value[0] / three_count, 4) * 100)[:4] + '%'}</td>
            <td>{all_count}</td>
            <td>{str(round(three_value[0] / all_count, 4) * 100)[:4] + '%'}</td>
        </tr>
        <tr align="center">
            <td>{three_index[1]}</td>
            <td>{three_value[1]}</td>
            <td>{three_count}</td>
            <td>{str(round(three_value[1] / three_count, 4) * 100)[:4] + '%'}</td>
            <td>{all_count}</td>
            <td>{str(round(three_value[1] / all_count, 4) * 100)[:4] + '%'}</td>
        </tr>
        <tr align="center">
            <td>{three_index[2]}</td>
            <td>{three_value[2]}</td>
            <td>{three_count}</td>
            <td>{str(round(three_value[2] / three_count, 4) * 100)[:4] + '%'}</td>
            <td>{all_count}</td>
            <td>{str(round(three_value[2] / all_count, 4) * 100)[:4] + '%'}</td>
        </tr>
        <tr align="center">
            <td>{three_index[3]}</td>
            <td>{three_value[3]}</td>
            <td>{three_count}</td>
            <td>{str(round(three_value[3] / three_count, 4) * 100)[:4] + '%'}</td>
            <td>{all_count}</td>
            <td>{str(round(three_value[3] / all_count, 4) * 100)[:4] + '%'}</td>
        </tr>
        <tr align="center">
            <td>{three_index[4]}</td>
            <td>{three_value[4]}</td>
            <td>{three_count}</td>
            <td>{str(round(three_value[4] / three_count, 4) * 100)[:4] + '%'}</td>
            <td>{all_count}</td>
            <td>{str(round(three_value[4] / all_count, 4) * 100)[:4] + '%'}</td>
        </tr>
        <tr align="center">
            <td>{three_index[5]}</td>
            <td>{three_value[5]}</td>
            <td>{three_count}</td>
            <td>{str(round(three_value[5] / three_count, 4) * 100)[:4] + '%'}</td>
            <td>{all_count}</td>
            <td>{str(round(three_value[5] / all_count, 4) * 100)[:4] + '%'}</td>
        </tr>
        <tr align="center">
            <td>{three_index[6]}</td>
            <td>{three_value[6]}</td>
            <td>{three_count}</td>
            <td>{str(round(three_value[6] / three_count, 4) * 100)[:4] + '%'}</td>
            <td>{all_count}</td>
            <td>{str(round(three_value[6] / all_count, 4) * 100)[:4] + '%'}</td>
        </tr>
        <tr align="center">
            <td>{three_index[7]}</td>
            <td>{three_value[7]}</td>
            <td>{three_count}</td>
            <td>{str(round(three_value[7] / three_count, 4) * 100)[:4] + '%'}</td>
            <td>{all_count}</td>
            <td>{str(round(three_value[7] / all_count, 4) * 100)[:4] + '%'}</td>
        </tr>
        <tr align="center">
            <td>{three_index[8]}</td>
            <td>{three_value[8]}</td>
            <td>{three_count}</td>
            <td>{str(round(three_value[8] / three_count, 4) * 100)[:4] + '%'}</td>
            <td>{all_count}</td>
            <td>{str(round(three_value[8] / all_count, 4) * 100)[:4] + '%'}</td>
        </tr>
        <tr align="center">
            <td>{three_index[9]}</td>
            <td>{three_value[9]}</td>
            <td>{three_count}</td>
            <td>{str(round(three_value[9] / three_count, 4) * 100)[:4] + '%'}</td>
            <td>{all_count}</td>
            <td>{str(round(three_value[9] / all_count, 4) * 100)[:4] + '%'}</td>
        </tr>
        <tr align="center">
            <td>{three_index[10]}</td>
            <td>{three_value[10]}</td>
            <td>{three_count}</td>
            <td>{str(round(three_value[10] / three_count, 4) * 100)[:4] + '%'}</td>
            <td>{all_count}</td>
            <td>{str(round(three_value[10] / all_count, 4) * 100)[:4] + '%'}</td>
        </tr>
        <tr align="center">
            <td>{three_index[11]}</td>
            <td>{three_value[11]}</td>
            <td>{three_count}</td>
            <td>{str(round(three_value[11] / three_count, 4) * 100)[:4] + '%'}</td>
            <td>{all_count}</td>
            <td>{str(round(three_value[11] / all_count, 4) * 100)[:4] + '%'}</td>
        </tr>
        <tr align="center">
            <td>{three_index[12]}</td>
            <td>{three_value[12]}</td>
            <td>{three_count}</td>
            <td>{str(round(three_value[12] / three_count, 4) * 100)[:4] + '%'}</td>
            <td>{all_count}</td>
            <td>{str(round(three_value[12] / all_count, 4) * 100)[:4] + '%'}</td>
        </tr>
        <tr align="center">
            <td>{three_index[13]}</td>
            <td>{three_value[13]}</td>
            <td>{three_count}</td>
            <td>{str(round(three_value[13] / three_count, 4) * 100)[:4] + '%'}</td>
            <td>{all_count}</td>
            <td>{str(round(three_value[13] / all_count, 4) * 100)[:4] + '%'}</td>
        </tr>
        <tr align="center">
            <td>{three_index[14]}</td>
            <td>{three_value[14]}</td>
            <td>{three_count}</td>
            <td>{str(round(three_value[14] / three_count, 4) * 100)[:4] + '%'}</td>
            <td>{all_count}</td>
            <td>{str(round(three_value[14] / all_count, 4) * 100)[:4] + '%'}</td>
        </tr>
        <tr align="center">
            <td>{three_index[15]}</td>
            <td>{three_value[15]}</td>
            <td>{three_count}</td>
            <td>{str(round(three_value[15] / three_count, 4) * 100)[:4] + '%'}</td>
            <td>{all_count}</td>
            <td>{str(round(three_value[15] / all_count, 4) * 100)[:4] + '%'}</td>
        </tr>
        <tr align="center">
            <td>{three_index[16]}</td>
            <td>{three_value[16]}</td>
            <td>{three_count}</td>
            <td>{str(round(three_value[16] / three_count, 4) * 100)[:4] + '%'}</td>
            <td>{all_count}</td>
            <td>{str(round(three_value[16] / all_count, 4) * 100)[:4] + '%'}</td>
        </tr>
        <tr align="center">
            <td>{three_index[17]}</td>
            <td>{three_value[17]}</td>
            <td>{three_count}</td>
            <td>{str(round(three_value[17] / three_count, 4) * 100)[:4] + '%'}</td>
            <td>{all_count}</td>
            <td>{str(round(three_value[17] / all_count, 4) * 100)[:4] + '%'}</td>
        </tr>
        <tr align="center">
            <td>{three_index[18]}</td>
            <td>{three_value[18]}</td>
            <td>{three_count}</td>
            <td>{str(round(three_value[18] / three_count, 4) * 100)[:4] + '%'}</td>
            <td>{all_count}</td>
            <td>{str(round(three_value[18] / all_count, 4) * 100)[:4] + '%'}</td>
        </tr>
        <tr align="center">
            <td>{three_index[19]}</td>
            <td>{three_value[19]}</td>
            <td>{three_count}</td>
            <td>{str(round(three_value[19] / three_count, 4) * 100)[:4] + '%'}</td>
            <td>{all_count}</td>
            <td>{str(round(three_value[19] / all_count, 4) * 100)[:4] + '%'}</td>
        </tr>
    </table>
    '''


    send(text, name)


def send(text, name):
    # 网易163邮箱
    HOST = 'smtp.163.com'
    # 2> 配置服务的端口，默认的邮件端口是25.
    PORT = '25'
    # 3> 指定发件人和收件人。
    FROM = '17612472355@163.com'
    TO = ','.join(['wangjinglei@baidaweb.cn','17612472355@baidaweb.cn','wanggang@baidaweb.cn','lizhiqiao@baidaweb.cn','lemon@baidaweb.cn','Zichang@baidaweb.cn'])
    TO = '17612472355@baidaweb.cn'
    # 4> 邮件标题
    SUBJECT = '各级标签占比邮件'
    # 5> 邮件内容
    msg = MIMEText(text, 'html', 'utf-8')
    # 创建邮件发送对象
    # 普通的邮件发送形式
    smtp_obj = smtplib.SMTP()

    # 需要进行发件人的认证，授权。
    # smtp_obj就是一个第三方客户端对象
    smtp_obj.connect(host=HOST, port=PORT)

    # 如果使用第三方客户端登录，要求使用授权码，不能使用真实密码，防止密码泄露。
    res = smtp_obj.login(user=FROM, password='lishuo32784322')
    # print('登录结果：', res)

    msg['Subject'] = f'{name}标签占比邮件'
    msg['To'] = ','.join(['wangjinglei@baidaweb.cn','17612472355@baidaweb.cn','wanggang@baidaweb.cn','lizhiqiao@baidaweb.cn','lemon@baidaweb.cn','Zichang@baidaweb.cn'])
#    msg['To'] = ','.join(['17612472355@baidaweb.cn'])
    msg['From'] = '17612472355@163.com'
    smtp_obj.sendmail(from_addr=FROM, to_addrs=TO.split(','), msg=msg.as_string())


Pool(3).map(f, ['(rec_status = -2 or rec_status = -4)', 'rec_status = -2', 'rec_status = -4'])
