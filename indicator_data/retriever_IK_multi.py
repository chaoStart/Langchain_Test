
import os
import pandas as pd
from elasticsearch import Elasticsearch

# 1. 连接 Elasticsearch
es = Elasticsearch("http://localhost:9200", verify_certs=False)
index_name = 'datang_docs_index1'
# 示例查询：查找包含“文化”相关的条目
query = {
    "query": {
        "multi_match": {
            "query": "煤机完成率",  # 你的检索关键词
            "fields": ["title", "content"],
            "analyzer": "ik_max_word"
        }
    }
}

res = es.search(index=index_name, body=query, size=50)

print("\n查询结果 Top 50：")
for hit in res['hits']['hits']:
    print(f"- Index: {hit['_source']['index']}\n  "
          f"Title: {hit['_source']['title']}\n  "
          f"Content: {hit['_source']['content']}\n  "
          f"Score: {hit['_score']}\n")


