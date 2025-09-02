
from elasticsearch import Elasticsearch
es = Elasticsearch("http://localhost:9200", verify_certs=False)
index_name = "suzhou_storage_dataset_big_chunk1"

import os
import openai
from dotenv import load_dotenv
load_dotenv()
os.environ["OPENAI_BASE_URL"] = "http://10.3.24.46:9997/v1"
os.environ["OPENAI_API_KEY"] = "123"

# 0.初始化嵌入模型对象
client = openai.Client()

def get_embedding(text):
    res = client.embeddings.create(input=text, model="bge-large-zh-v1.5")
    print("触发N次？",res.data[0].embedding)
    return res.data[0].embedding

# 用户输入问题
query_text = "基数电的上网电价（含税）是多少?"
# 获取向量嵌入
query_vector = get_embedding(query_text)
# 组合查询：文本 + 向量
query = {
    "bool": {
        "must": [
            {
                "multi_match": {
                    "query": query_text,
                    "fields": ["table_name", "row_name", "column_name"],
                    "analyzer": "ik_max_word"
                }
            }
        ],
        "should": [
            {
                "script_score": {
                    "query": {"match_all": {}},
                    "script": {
                        "source": "cosineSimilarity(params.query_vector, 'embedding') + 1.0",
                        "params": {"query_vector": query_vector}
                    }
                }
            }
        ]
    }
}

res = es.search(index=index_name, query=query, size=10)

# 打印结果
for hit in res["hits"]["hits"]:
    score = hit["_score"]
    source = hit["_source"]
    row_or_column = source['row_name'] if source.get('row_name') else source['column_name']
    print(f"[{score:.2f}] {source['table_name']} - {source['sheet_type']} - {row_or_column}\n\n")

es.close()