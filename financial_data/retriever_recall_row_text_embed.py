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
query_text = "利润表中营业总收入的上年同期累计是多少"
query_vector = get_embedding(query_text)

# ====== 1. 关键词检索 ======
keyword_query = {
    "bool": {
        "must": [  # 必须包含 row_name 字段
            {"exists": {"field": "row_name"}}
        ],
        "should": [
            {
                "match": {
                    "row_name": {
                        "query": query_text,
                        "boost": 1,
                        "analyzer": "ik_max_word"
                    }
                }
            },
            {
                "match": {
                    "table_name": {
                        "query": query_text,
                        "boost": 2,
                        "analyzer": "ik_max_word"
                    }
                }
            }
        ],
        "minimum_should_match": 1
    }
}
keyword_res = es.search(index=index_name, query=keyword_query, size=50)["hits"]["hits"]

# ====== 2. 向量检索 ======
vector_query = {
    "script_score": {
        "query": {
            "bool": {
                "must": [
                    {"exists": {"field": "row_name"}}   # 仅当 row_name 存在
                ]
            }
        },
        "script": {
            "source": "cosineSimilarity(params.query_vector, 'embedding') + 1.0",
            "params": {"query_vector": query_vector}
        }
    }
}

vector_res = es.search(index=index_name, query=vector_query, size=50)["hits"]["hits"]

# ====== 3. 合并逻辑 ======
merged = {}
# 先放关键词结果
for hit in keyword_res:
    key = (hit["_source"]["table_name"], hit["_source"]["row_name"])
    merged[key] = {
        "source": hit["_source"],
        "bm25_score": hit["_score"],
        "vector_score": 0.0  # 暂无
    }

# 再放向量结果:向量召回的可能会table_name+row_name，或者table_name+column_name
for hit in vector_res:
    key = (hit["_source"]["table_name"], hit["_source"]["row_name"] if "row_name" in hit["_source"] else hit["_source"].get("column_name", ""))
    if key in merged:
        merged[key]["vector_score"] = hit["_score"]
    else:
        merged[key] = {
            "source": hit["_source"],
            "bm25_score": 0.0,
            "vector_score": hit["_score"]
        }

# 计算新得分
final_results = []
for key, val in merged.items():
    bm25 = val["bm25_score"]
    vec = val["vector_score"]

    if bm25 > 0 and vec > 0:  # 同时命中
        score = bm25 * 0.5 + vec * 0.5
    else:  # 只命中一个
        score = max(bm25, vec) * 0.5

    row_or_column = "row_name" if "row_name" in val["source"] else "column_name"
    content = val["source"].get("row_name") if "row_name" in val["source"] else val["source"].get("column_name", "")
    final_results.append({
        "score": score,
        "table_name": val["source"]["table_name"],
        "sheet_type": val["source"].get("sheet_type", ""),
        row_or_column: content
    })

# 排序
final_results.sort(key=lambda x: x["score"], reverse=True)
print("关键词+向量召回的数据记录合计总数:", len(final_results))
# 取前10个
top_10_results = final_results[:10]

# 输出
for r in top_10_results:
    content = r["row_name"] if "row_name" in r else r["column_name"]
    print(f"[{r['score']:.2f}] {r['table_name']} - 数据类型:{r['sheet_type']} - {content}\n\n")

es.close()
