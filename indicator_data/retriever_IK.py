
import os
import pandas as pd
from elasticsearch import Elasticsearch

# 1. 连接 Elasticsearch
es = Elasticsearch("http://localhost:9200", verify_certs=False)
index_name = 'datang_index_name'
# 示例查询：查找包含“文化”相关的条目
query = {
    "query": {
        "match": {
            "value": {
                "query": "供电量",       # 会被 IK 分词
                "analyzer": "ik_max_word"  # 用最大粒度分词提升召回
            }
        }
    }
}

res = es.search(index=index_name, body=query,size=200) # 控制最多返回100条数据
print("查询结果：")
for hit in res['hits']['hits']:
    print(f"- {hit['_source']['value']} (score: {hit['_score']})")
