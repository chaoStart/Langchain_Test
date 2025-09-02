
from elasticsearch import Elasticsearch

# 1. 连接 Elasticsearch
es = Elasticsearch("http://localhost:9200", verify_certs=False)
index_name = 'suzhou_storage_dataset_big_chunk'

# 2.简单测试是否可以查询检索
response = es.search(index=index_name, body={
    "query": {
        "match": {
            "table_name": "经济增加值明细表",
        }
    },
    "_source": ["sheet_name"],  # 只返回 sheet_name 字段
    "size": 15,
})
sheet_names = [hit["_source"]["sheet_name"] for hit in response["hits"]["hits"]]
print("✅ 根据列名返回查询的检索信息",sheet_names)

# 3. 使用 multi_match 查询 row_name、column_name 和 content 字段，返回匹配的 sheet_name
def search_sheet_name_by_keywords(keywords, top_k=5):
    query_body = {
        "query": {
            "bool": {
                "should": [
                    {"match": {"row_name": {"query": keywords, "boost": 1, "analyzer": "ik_max_word"}}},
                    {"match": {"column_name": {"query": keywords, "boost": 1, "analyzer": "ik_max_word"}}},
                    {"match": {"table_name": {"query": keywords, "boost": 2, "analyzer": "ik_max_word"}}}
                ],
                "minimum_should_match": 1
            }
        },
        "_source": ["sheet_name"],  # 只返回 sheet_name 字段
        "size": top_k
    }

    response = es.search(index=index_name, body=query_body)
    sheet_names = [hit["_source"]["sheet_name"] for hit in response["hits"]["hits"]]
    return sheet_names


# 示例调用：根据关键词查找工作表名称
question = "现金流量表中收回投资收到的现金对应的本年累计"
matched_sheets = search_sheet_name_by_keywords(question)
print("🔍 根据行名+列名 匹配的工作表名称：", matched_sheets)
print("------------------------------------------------")
# 验证当前索引中是否有文档
print("当前索引文档总数：", es.count(index=index_name)["count"])