from elasticsearch import Elasticsearch
import os
import openai
from dotenv import load_dotenv

load_dotenv()
os.environ["OPENAI_BASE_URL"] = "http://10.3.24.46:9997/v1"
os.environ["OPENAI_API_KEY"] = "123"

es = Elasticsearch("http://localhost:9200", verify_certs=False)
index_name = "suzhou_storage_dataset_big_chunk1"

# 初始化嵌入模型
client = openai.Client()

def get_embedding(text):
    res = client.embeddings.create(input=text, model="bge-large-zh-v1.5")
    return res.data[0].embedding


# 用户查询
query_text = "基数电的上网电价（含税）是多少"
query_vector = get_embedding(query_text)

# ====== 1. 向量检索 ======
vector_query = {
    "script_score": {
        "query": {"match_all": {}},
        "script": {
            "source": "cosineSimilarity(params.query_vector, 'embedding') + 1.0",
            "params": {"query_vector": query_vector}
        }
    }
}
vector_res = es.search(index=index_name, query=vector_query, size=20)["hits"]["hits"]

# 输出
for r in vector_res:
    print(f"[{r['_score']:.2f}] {r['table_name']} - {r['sheet_type']} - {r['row_name']}")

es.close()
