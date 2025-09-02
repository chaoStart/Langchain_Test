import os
import openai
from dotenv import load_dotenv
from elasticsearch import Elasticsearch
from utils import query_by_row_text_embedded, query_by_column_text_embedded, \
    get_all_column_name, get_sheet_name_unit

load_dotenv()
os.environ["OPENAI_BASE_URL"] = "http://10.3.24.46:9997/v1"
os.environ["OPENAI_API_KEY"] = "123"

es = Elasticsearch("http://localhost:9200", verify_certs=False)
index_name = "suzhou_storage_dataset_big_chunk1"
# 初始化嵌入模型
client = openai.Client()

print(es.indices.get_mapping(index=index_name))

def get_embedding(text):
    res = client.embeddings.create(input=text, model="bge-large-zh-v1.5")
    return res.data[0].embedding


def get_high_score(results_list):
    # 获取高分行召回数据
    record_high_score = []
    # 获取第一个得分最高最相关的数据
    score1 = results_list[0]["score"]
    for item in results_list:
        score_x = item["score"]
        if (score1 - score_x) / score1 <= 0.3:
            record_high_score.append(item)
    return record_high_score


def same_sheet_content_merged(merge_sheet_row_or_column):
    # 方法一：defaultdict
    from collections import defaultdict
    merged = defaultdict(list)
    for d in merge_sheet_row_or_column:
        for k, v in d.items():
            merged[k].extend(v)

    print("相同工作表sheet_name合并后的行名称/列名称数据", dict(merged))
    return merged


# 用户查询
# query_text = "管理费中核算的研究与开发费是多少"
# query_text = "8月经济增加值明细表的调整后资本是多少"
query_text = "我想知道8月基数电的上年同期"
# query_text = "利润表中基本每股收益的上年同期累计、本月金额和本年累计是多少"
# query_text = "现金流量表中投资活动产生的现金流量的本年累计和上年同期累计是多少"
query_vector = get_embedding(query_text)

# 行文本+工作表文本进行召回
row_results = query_by_row_text_embedded(query_text, query_vector)

# 判断返回的行数据是否为空
if len(row_results) != 0:
    record_high_row_score = get_high_score(row_results)
else:
    record_high_row_score = []

# 筛选重复的工作表名称，返回无重复的工作表名称+公司名称+时间日期
location_sheet_info = {}
for v in record_high_row_score:
    # 判断当前的数据是否和存储的sheet_info有相同工作表、公司名称、日期
    if v["table_name"] in location_sheet_info:
        continue
    else:
        key = v["table_name"]
        location_sheet_info[key] = {
            "company_name": v["company_name"],
            "table_name": key,
            "sheet_name": v["sheet_name"],
            "data_month": v["data_month"]
        }

print("当前的工作表有:", location_sheet_info, "\n--------\n")

# 存储所有行数据
merge_all_row_list = []
for v in location_sheet_info.values():
    for each_row in record_high_row_score:
        if v["table_name"] == each_row["table_name"]:  # 说明sheet_name中匹配到了高分工作表中table_name
            # 如果merge_row_list中历史数据中没有table，则要在merge_row_list中新增一个table_name:row_name
            merge_all_row_list.append({v["table_name"]: [each_row["row_name"].split('\n')[0]]})

# 存储所有列数据
merge_all_column_list = []
for v in location_sheet_info.values():
    column_results = query_by_column_text_embedded(query_text, query_vector, v)
    # 判断res是否有召回到数据
    if len(column_results) != 0:
        record_high_column_score = get_high_score(column_results)  # 筛选高分列数据
        for each_column in record_high_column_score:
            if v["table_name"] == each_column["table_name"]:  # 说明sheet_name中匹配到了高分工作表中table_name
                # 如果merge_all_column_list中历史数据中没有table_name:column_name，则要在merge_all_column_list中新增一个table_name:column_name
                merge_all_column_list.append({v["table_name"]: [each_column["column_name"]]})
        # 判断工作表含有“单位”
        unit = get_sheet_name_unit(v)
        if unit:
            merge_all_column_list.append({v["table_name"]: [unit]})

    else:  # 召回column_data数据失败，返回该工作表的全部column_name数据
        all_column_as_name = get_all_column_name(v)
        merge_all_column_list.append({v["table_name"]: all_column_as_name})
        print("没有匹配到列名称，返回该工作表的全部列名称~")

# 把相同工作表下的行名称合并在一个list数组里面
row_merged = same_sheet_content_merged(merge_all_row_list)

# 把相同工作表下的列名称合并在一个list数组里面
column_merged = same_sheet_content_merged(merge_all_column_list)

# 通过遍历location_sheet_info中的所有工作表sheet_name，组合工作表中的row_merged+column_merged
complete_row_column = []
for sheet_v in location_sheet_info.values():
    # 取出table_name
    table_n = sheet_v["table_name"]
    if table_n in row_merged and table_n in column_merged:
        key = table_n
        row_column = {key: {"company_name": sheet_v["company_name"], "table_name": sheet_v["table_name"],
                            "sheet_name": sheet_v["sheet_name"], "data_month": sheet_v["data_month"],
                            "row_list": row_merged[table_n], "column_list": column_merged[table_n]}}
        complete_row_column.append(row_column)

    else:
        continue

print("完整的全部的工作表对应的行列数据:", complete_row_column, '\n')
print("查询问题:", query_text, "\n")
for data in complete_row_column:
    for k, v in data.items():
        print(
            f'sheet_name:{v["sheet_name"]}--company_name:{v["company_name"]}--data_month:{v["data_month"]}\nrow_list:{v["row_list"]}\ncolumn_list:{v["column_list"]}\n')
