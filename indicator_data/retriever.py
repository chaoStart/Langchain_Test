from elasticsearch import Elasticsearch

# 连接 Elasticsearch
es = Elasticsearch("http://localhost:9200")

# 索引名称
index_name = "datang_index_name"

# 要查询的关键词
search_value = "完成率"

# 构造查询 DSL（match 会自动分词）
query_body = {
    "query": {
        "match": {
            "value": search_value
        }
    }
}

# 执行搜索
response = es.search(index=index_name, body=query_body)

# 判断是否存在并输出结果
hits = response["hits"]["hits"]

if hits:
    print(f"找到 {len(hits)} 条匹配记录：")
    for hit in hits:
        print(hit["_source"])
else:
    print("未找到匹配记录。")
