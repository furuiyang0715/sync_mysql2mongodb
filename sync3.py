# 生成指定格式的交易表
# 关键点是:注意交易日的生成规则
import os
import pymongo
import datetime


MONGO_URI = os.environ.get("MONGO_URI", "mongodb://127.0.0.1:27017/jzquant")
mongo = pymongo.MongoClient(MONGO_URI)

# 获取所有的证券代码
n_coll = mongo["datacenter"]["const_specialnotice"]
all_secucode_list = n_coll.find({}, {"SecuCode": 1}).distinct("SecuCode")

# 获取所有的日期
all_date_list = list()
# 设置起始日期和终止日期 写死的
min_date = datetime.datetime(1990, 12, 19)
max_date = datetime.datetime(2099, 12, 31)
# 生成起始日期和终止日期之间的日期列表
while min_date <= max_date:
    all_date_list.append(min_date.strftime("%Y%m%d"))
    min_date += datetime.timedelta(days=1)


# 获取非交易日列表
free_coll = mongo["datacenter"]["const_tradingday"]
free_date_list = free_coll.find({"IfTradingDay": 2, "SecuMarket": 83},
                                {"Date": 1}).distinct("Date")
free_str_date_list = []
for date in free_date_list:
    free_str_date_list.append(date.strftime("%Y%m%d"))

# # 插入数据
# calendar_coll = mongo["datacenter"]["calendar"]
# for code in all_secucode_list[:3]:  # 针对具体的某只证券
#     print("begin!", code, "-"*88)
#     # 获取停牌后复牌的交易日
#     normal_begin_date = list(n_coll.find({"TypeCodeII": 1703, "TypeCodeI": 18, "SecuCode": code},
#                                          {"BeginDate": 1, "EndDate": 1}))
#     normal_date_list = []
#     for i in normal_begin_date:
#         if i.get("BeginDate") != i.get("EndDate"):
#             print("wrong......")  # 校验停牌后复牌的起始和终止日期都在同一天
#         else:
#             # print("success......")
#             # 拼接停牌后复牌的交易日列表
#             normal_date_list.append(i.get("BeginDate").strftime("%Y%m%d"))
#
#     # 获取该证券的所有停牌日期
#     sus_res_list = list(n_coll.find({"TypeCodeII": 1702, "TypeCodeI": 18, "SecuCode": code},
#                                     {"BeginDate": 1, "EndDate": 1}))
#
#     today = datetime.datetime.combine(datetime.date.today(), datetime.time.min)
#
#     sus_date_list = list()
#     for sus_res in sus_res_list:
#         begin_date = sus_res.get("BeginDate")
#         assert begin_date is not None, "起始停牌日为空......"
#
#         end_date = sus_res.get("EndDate")
#         if not end_date:
#             end_date = today  # 若不存在终止停牌日期 设置为当前日期
#
#         while begin_date <= end_date:
#             sus_date_list.append(begin_date.strftime("%Y%m%d"))
#             begin_date += datetime.timedelta(days=1)
#
#     # 重要的一步：（1）去重； （2）和非交易日取并集； （3）减去其中的正常交易日
#     sus_date_list =list(set(sus_date_list) | set(free_str_date_list) - set(normal_date_list))
#
#     # 入库
#     insert_list = list()
#     for date in all_date_list:
#         # 如果是停牌日
#         if date in sus_date_list:
#             insert = {"code": code, "date": datetime.datetime.strptime(date, "%Y%m%d"),
#                       "ok": False, "date_int": date}
#         else:  # 交易日
#             insert = {"code": code, "date": datetime.datetime.strptime(date, "%Y%m%d"),
#                       "ok": True, "date_int": date}
#         insert_list.append(insert)
#
#     try:
#         calendar_coll.insert_many(insert_list)
#         print("insert many success......")
#     except Exception as e:
#         print("---"*33, e)

### 数据校验
check_coll = mongo["stock"]["calendar"]
calendar_coll = mongo["datacenter"]["calendar"]
for code in all_secucode_list[:3]:  # 针对具体的某只证券
    print("begin!", code, "-"*88)
    # 获取停牌后复牌的交易日
    normal_begin_date = list(n_coll.find({"TypeCodeII": 1703, "TypeCodeI": 18, "SecuCode": code},
                                         {"BeginDate": 1, "EndDate": 1}))
    normal_date_list = []
    for i in normal_begin_date:
        if i.get("BeginDate") != i.get("EndDate"):
            print("wrong......")  # 校验停牌后复牌的起始和终止日期都在同一天
        else:
            # print("success......")
            # 拼接停牌后复牌的交易日列表
            normal_date_list.append(i.get("BeginDate").strftime("%Y%m%d"))

    # 获取该证券的所有停牌日期
    sus_res_list = list(n_coll.find({"TypeCodeII": 1702, "TypeCodeI": 18, "SecuCode": code},
                                    {"BeginDate": 1, "EndDate": 1}))

    today = datetime.datetime.combine(datetime.date.today(), datetime.time.min)

    sus_date_list = list()
    for sus_res in sus_res_list:
        begin_date = sus_res.get("BeginDate")
        assert begin_date is not None, "起始停牌日为空......"

        end_date = sus_res.get("EndDate")
        if not end_date:
            end_date = today  # 若不存在终止停牌日期 设置为当前日期

        while begin_date <= end_date:
            sus_date_list.append(begin_date.strftime("%Y%m%d"))
            begin_date += datetime.timedelta(days=1)

    # 重要的一步：（1）去重； （2）和非交易日取并集； （3）减去其中的正常交易日
    sus_date_list = list(set(sus_date_list) | set(free_str_date_list) - set(normal_date_list))

    # 获取在校验数据库中的数据
    for date_int in all_date_list[365*3+59: 365*27+365+19+1]:
        print(date_int)
        # print(all_date_list[365*3+59])  # 19940215
        # print(all_date_list[365*27+365+19]) # 20181231
        o_ok = check_coll.find({"date_int": int(date_int)}, {"ok": 1}).distinct("ok")
        if o_ok:
            o_ok = o_ok[0]
            c_ok = calendar_coll.find({"date_int": date_int}, {"ok": 1}).next().get("ok")

            if not c_ok == o_ok:
                print(c_ok, o_ok)
                print(code, date_int, "!"*100)
            else:
                pass
        else:
            print("无o_ok", code, date_int)

