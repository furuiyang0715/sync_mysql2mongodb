import decimal
import pymysql
from pymongo import MongoClient


class SyncData:
    def __init__(self):
        self.mysql_host = "139.159.176.118"
        self.mysql_username = "dcr"
        self.mysql_password = ""
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
                num = 10000
                start = 0
                while True:
                    query_sql = """
                    select * from {} where id > {} limit {};""".format(table_name, start, num)
                    cursor.execute(query_sql)
                    res = cursor.fetchall()
                    if not res:
                        break
                    start += num
                    for column in res:
                        column_dict = self.zip_doc_dict(name_list, column)
                        yield column_dict
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
                # 对每个字典数据进行数据类型的检验
                each_checked_sql_table_data = self.check_each_sql_table_data(each_sql_table_data)
                res = mongo_collection.insert_one(each_checked_sql_table_data)
                print("insert success", res)
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
        sql_table_name_list = ['bas_induinfo']  # √ 484
        sql_table_name_list = ['comcn_balancesheet']  #

        for table_name in sql_table_name_list:
            table_name_list = self.generate_sql_table_column_names(conn, self.mysql_DBname,
                                                                   table_name)

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


if __name__ == "__main__":
    rundemo = SyncData()
    rundemo.sync_data()

