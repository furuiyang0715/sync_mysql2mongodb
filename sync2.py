import time
import decimal
import pymysql
from pymongo import MongoClient


class SyncData:
    def __init__(self):
        self.mysql_host = "139.159.176.118"
        self.mysql_username = "ruiyang"
        self.mysql_password = "xxxxxxxxx"
        self.mysql_DBname = "datacenter"
        self.mysql_port = 3306

        self.mongo_host = "localhost"
        self.mongo_port = 27017
        self.mongo_DBname = "datacenter"

    def generate_mysqlconnection(self):
        return pymysql.connect(
            host=self.mysql_host,
            port=self.mysql_port,
            user=self.mysql_username,
            password=self.mysql_password,
            charset='utf8mb4',
            db=self.mysql_DBname
        )

    def generate_mongo_collection(self, db_name, col_name):
        client = MongoClient(self.mongo_host, self.mongo_port)
        db = client["{}".format(db_name)]
        col = db["{}".format(col_name)]
        return col

    @staticmethod
    def generate_sql_head_name_list(connection, db_name, table_name):
        """
        生成mysql中一个table中所有字段组成的列表
        :param connection:
        :param db_name:
        :param table_name:
        :return:
        """
        query_sql = """
        select COLUMN_NAME, DATA_TYPE, column_comment from information_schema.COLUMNS 
        where table_name="{}" and table_schema="{}";
        """.format(table_name, db_name)

        head_name_list = list()
        try:
            with connection.cursor() as cursor:
                cursor.execute(query_sql)
                res = cursor.fetchall()
                for i in res:
                    head_name_list.append(i[0])
        finally:
            connection.commit()
        return head_name_list

    def generate_sql_table_length(self, connection, table_name):
        """
        计算数据库中的行数
        :param connection:
        :param table_name:
        :return:
        """
        query_sql = """
                select count(*) from {};
                """.format(table_name)
        try:
            with connection.cursor() as cursor:
                cursor.execute(query_sql)
                res = cursor.fetchall()
                table_length = res[0]
        finally:
            connection.commit()
        return table_length

    def generate_mongo_collection_length(self, mongo_collection):
        """
        查询集合中文档总数
        :param mongo_collection:
        :return:
        """
        return mongo_collection.find().count()

    def generate_sql_table_datas_list(self, connection, table_name, name_list):
        try:
            with connection.cursor() as cursor:
                # 遍历生成器的时候 每次返回10000条待插入数据
                # 原因：如果不适用 generate 几百万条数据 容易爆内存...
                num = 10000
                start = 0
                while True:
                    # 注意： 不能使用id作为偏移标准 在mysql表id不连续的情况下 会造成多插数据
                    # query_sql = """
                    # select * from {} where id > {} limit {};""".format(table_name, start, num)
                    # eg. 0, 10000
                    #     10000, 10000 ...
                    query_sql = """
                    select * from {} limit {},{};""".format(table_name, start, num)

                    cursor.execute(query_sql)
                    # res 是每次从mysql取出的10000条（或者最后一次少于10000条）的数据
                    res = cursor.fetchall()
                    if not res:
                        break
                    start += num

                    # 生成器部分
                    # 每次 yield 出一个字典嵌套列表 使用 insert_many 插入
                    yield_column_list = list()
                    for column in res:
                        column_dict = self.zip_doc_dict(name_list, column)
                        yield_column_list.append(column_dict)
                    yield yield_column_list
        finally:
            connection.commit()

    @staticmethod
    def zip_doc_dict(name_list, column_tuple):
        """
        将mysql字段和每一行数据组成一个字典 便于mongodb插入
        :param name_list:
        :param column_tuple:
        :return:
        """
        assert len(name_list) == len(column_tuple)
        name_tuple = tuple(name_list)
        column_dict = dict(zip(name_tuple, column_tuple))
        return column_dict

    def check_each_sql_table_data(self, dict_data):
        for key, value in dict_data.items():
            if type(value) == decimal.Decimal:
                # 如果小数点的长度为 0 就转化为 int
                if value.as_tuple().exponent == 0:
                    dict_data[key] = int(value)
                else:  # 如果小数点位数不为 0 就转化为 float
                    dict_data[key] = float(value)
        return dict_data

    def write_datas2mongo(self, mongo_collection, sql_table_datas_list):
        try:
            for yield_list in sql_table_datas_list:
                # yield_list 是一个生成器 yield 出的 list
                # j_list 是通过数据类型转换可进行插入的 list
                j_list = list()
                for j in yield_list:
                    # j 是每一个 需要进行数值 check 的 dict
                    j = self.check_each_sql_table_data(j)
                    j_list.append(j)
                res = mongo_collection.insert_many(j_list)
                print("--- insert many success --- ", res)
        except Exception as e:
            print(e)

    def gen_sql_table_name_list(self, connection):
        query_sql ="""
        select table_name from information_schema.tables where table_schema="{}";
        """.format(self.mysql_DBname)
        sql_table_name_list = list()
        try:
            with connection.cursor() as cursor:
                cursor.execute(query_sql)
                res = cursor.fetchall()
                for column in res:
                    sql_table_name_list.append(column[0])
        finally:
            connection.commit()
        return sql_table_name_list

    def sync_data(self):
        """
        同步数据
        :return:
        """
        # 创建 mysql 连接对象 conn
        conn = self.generate_mysqlconnection()
        # 拿到mysql数据库中的所有table的列表
        sql_table_name_list = self.gen_sql_table_name_list(conn)

        # 遍历每一个table_name, 实现mysql中一个table--->mongodb中的一个collection的映射
        for table_name in sql_table_name_list:
            # 生成mysql table 的字段列表，它们作为之后生成的每一个 mongodb doc 的键
            head_name_list = self.generate_sql_head_name_list(conn, self.mysql_DBname, table_name)

            # type(sql_table_datas_list) --> generator
            # 一个每次返回部分待插入数据的生成器 在写入mongodb时遍历这个生成器
            sql_table_datas_list = self.generate_sql_table_datas_list(conn, table_name,
                                                                      head_name_list)
            # 创建一个mongodb数据库的连接对象
            mongo_collection = self.generate_mongo_collection(self.mongo_DBname, table_name)
            # 向mongodb中插入数据
            self.write_datas2mongo(mongo_collection, sql_table_datas_list)

            # 校对两种数据库前后数量一致
            mongo_collection_length = self.generate_mongo_collection_length(mongo_collection)
            print("The generete mongodb collection length is: ", mongo_collection_length)
            sql_table_length = self.generate_sql_table_length(conn, table_name)[0]
            print("The current table lengtrh is:", sql_table_length)
            assert mongo_collection_length == sql_table_length

    def update_data(self):
        """
        考虑两种情况：
        (1) mysql 每一个 table 中的一行数据为最小变更 可以根据集合进行增删 update
        (2) 以字段为最小变更。需要拿到详细的一份更新日志
        :return:
        """
        pass


if __name__ == "__main__":
    rundemo = SyncData()
    print("=================开始同步数据========================")
    t1 = time.time()
    rundemo.sync_data()  # 同步数据, 只在有新的数据库表加入的时候同步一次
    print("=================同步数据结束========================")
    t2 = time.time()
    print("本次同步所用时间是：", (t2-t1)/60, "min")
    rundemo.update_data()  # 更新数据，当数据库表内容有增加或者修改的时候同步一次

