def conn():
    import pymysql
    connect = pymysql.connect(
        # host='120.92.77.36',  # 数据库地址
        host='localhost',  # 数据库地址
        # port=48368,  # 数据库端口
        port=3306,  # 数据库端口
        db='spider_resource',  # 数据库名
        # user='bd_ceshi',  # 数据库用户名
        user='root',  # 数据库用户名
        # passwd='BDqilingzhengfan1',  # 数据库密码
        passwd='123456',  # 数据库密码
        charset='utf8',  # 编码方式
        use_unicode=True)
    cursor = connect.cursor()
    connect.autocommit(True)
    return (connect,cursor)



if __name__ == '__main__':
    import random
    connect,cursor = conn()

    cursor.execute('select ip from ips')
    ip = random.choice(cursor.fetchall())[0]
    print(ip)