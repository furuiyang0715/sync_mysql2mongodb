import time
import decimal
import pymysql
from pymongo import MongoClient


class SyncData:
    def __init__(self):
        self.mysql_host = "139.159.176.118"
        self.mysql_username = "dcr"
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
    def generate_sql_table_column_names(connection, db_name, table_name):

        query_sql = """
        select COLUMN_NAME, DATA_TYPE, column_comment from information_schema.COLUMNS 
        where table_name="{}" and table_schema="{}";
        """.format(table_name, db_name)

        sql_table_col_name_list = list()
        try:
            with connection.cursor() as cursor:
                cursor.execute(query_sql)
                res = cursor.fetchall()
                for i in res:
                    sql_table_col_name_list.append(i[0])
        finally:
            connection.commit()
        return sql_table_col_name_list

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
        return mongo_collection.find().count()

    def generate_sql_table_datas_list(self, connection, table_name, name_list):
        try:
            with connection.cursor() as cursor:
                num = 1000
                start = 0
                while True:
                    # 坑... 不能使用id作为偏移标准 有的表的id是不连续唯一的 会造成多插数据
                    # query_sql = """
                    # select * from {} where id > {} limit {};""".format(table_name, start, num)
                    # eg. 0, 10000
                    #     10000, 10000 ...
                    query_sql = """
                    select * from {} limit {},{};""".format(table_name, start, num)

                    cursor.execute(query_sql)
                    res = cursor.fetchall()
                    if not res:
                        break
                    start += num

                    # 生成器部分
                    # 改进：之前是生成器每次 yield 出一个字典 使用 insert 插入
                    #      现在是每次 yield 出一个列表 使用 insert_many 插入
                    #      just try try try ...
                    yield_column_list = list()
                    for column in res:
                        column_dict = self.zip_doc_dict(name_list, column)
                        yield_column_list.append(column_dict)
                    yield yield_column_list
        finally:
            connection.commit()

    @staticmethod
    def zip_doc_dict(name_list, column_tuple):
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
            for each_sql_table_data in sql_table_datas_list:
                # each_sql_table_data 是一个 yield 出的 list
                # j_list 是组装好的需要重新进行插入的 list
                j_list = list()
                for j in each_sql_table_data:
                    # j 是每一个 需要进行check 的 dict
                    j = self.check_each_sql_table_data(j)
                    j_list.append(j)
                res = mongo_collection.insert_many(j_list)
                print("insert ----ss---- success", res)
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
        conn = self.generate_mysqlconnection()
        sql_table_name_list = self.gen_sql_table_name_list(conn)

        '''
        sql_table_name_list = ['bas_induinfo', 'comcn_balancesheet', 'comcn_cashflowsheet',
        'comcn_equitychangessheet', 'comcn_incomesheet', 'comcn_induinfo', 'comcn_qcashflowsheet',
        'comcn_qincomesheet', 'comcn_reservereportdate', 'comcn_sharesholder',
        'comcn_sharesstructurechange', 'cominv_interactive', 'const_companynamechange',
        'const_sector', 'const_secumain', 'const_specialnotice', 'const_stockabbrchange',
        'const_tradingday', 'hkland_historycashflow', 'hkland_historytradestat', 'hkland_shares',
        'index_baseinfo', 'index_baseinfosup', 'index_quot_day', 'index_weight',
        'stk_dividendinfo', 'stk_quotidx_day', 'stk_quotidxwind_day', 'stk_quotori_day']
        '''

        # 谨慎起见 trans one by one
        # sql_table_name_list = ['bas_induinfo']  # √ 484
        # sql_table_name_list = ['comcn_induinfo']  # √ 5643 1.40min
        # sql_table_name_list = ['stk_dividendinfo']  # √ 31925 8.22min
        # sql_table_name_list = ['index_baseinfosup']  # √ 683 0.37min
        # sql_table_name_list = ['index_baseinfo']  # √ 9096 6.27min
        # sql_table_name_list = ['hkland_historytradestat']  # √
        sql_table_name_list = ['index_quot_day']  # √

        for table_name in sql_table_name_list:
            print("table_name is ------> ", table_name)
            table_name_list = self.generate_sql_table_column_names(conn, self.mysql_DBname,
                                                                   table_name)
            print("table_name_list is ======> ", table_name_list)

            # type(sql_table_datas_list) --> generator
            sql_table_datas_list = self.generate_sql_table_datas_list(conn, table_name,
                                                                      table_name_list)

            mongo_collection = self.generate_mongo_collection(self.mongo_DBname, table_name)

            self.write_datas2mongo(mongo_collection, sql_table_datas_list)

            # 校对数量一致
            mongo_collection_length = self.generate_mongo_collection_length(mongo_collection)
            print("The generete mongodb collection length is: ", mongo_collection_length)
            sql_table_length = self.generate_sql_table_length(conn, table_name)[0]
            print("The current table lengtrh is:", sql_table_length)
            assert mongo_collection_length == sql_table_length

    def update_data(self):
        """
        考虑两种情况：
        (1) mysql 每一个 table 中的一行数据为最小变更 可以根据集合进行增删 update
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

