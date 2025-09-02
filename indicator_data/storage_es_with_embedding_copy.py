
import requests
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
# from utils.embedded_model import get_embedding

# 1.设置请求路径和请求参数
url = "http://10.44.2.104:9090/mainApi/syncplant-business-dataset/api/empoworx/dataset/storeconfig/process"

# 请求数据（JSON 格式）
payload = {
    "companyName": "大唐集团苏州分公司",
    "code": "",
    "queryType": "0",
    "rowPathList": [],
    "datasetIds": [],
    "columnParam": {},
    "name": "",
    "rowList": [],
    "columnList": [],
    "startTime": "2025-07-01",
    "endTime": "2025-08-19"
}

# 设置请求头（Content-Type 为 application/json）
headers = {
    "Content-Type": "application/json"
}
def fetch_data(url, payload, headers):
    try:
        response = requests.post(url, json=payload, headers=headers)
        response.raise_for_status()  # 检查HTTP请求是否成功
        return response.json()
    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error occurred: {http_err}")
    except Exception as err:
        print(f"Other error occurred: {err}")

# 2.请求外部数据集地址，获取初始数据
data = fetch_data(url, payload, headers)
if not data:
    exit()

response_data = data["data"]

indicator_name = []

# 3.对获取的数据进行预处理，包装为字典类型的指标数据——indicator_has_index
for k, v in response_data.items():
    if len(v["data"]) == 0:
        continue
    else:
        current_item_all_name = v["全指标名称"]
        indicator_name.extend(current_item_all_name)

print(indicator_name)

# 判断一下读取的指标名称是否存在相同名称
from collections import Counter

counter = Counter(indicator_name)

duplicates = [item for item, count in counter.items() if count > 1]
if duplicates:
    print("重复的字符串有:", duplicates)
else:
    print("没有重复的字符串")

# 对读取的指标名称进行去重
deduplication_indicator_name = list(set(indicator_name))
print("去重后的指标名称长度是:", len(deduplication_indicator_name))

import os
import openai
from dotenv import load_dotenv
load_dotenv()
os.environ["OPENAI_BASE_URL"] = "http://10.3.24.46:9997/v1"
os.environ["OPENAI_API_KEY"] = "123"

# 3.1 初始化嵌入模型对象
client = openai.Client()

def get_embedding(content_list):
    res = client.embeddings.create(input=content_list, model="bge-large-zh-v1.5")
    embeddings = [item.embedding for item in res.data]
    return embeddings


content_embedding_list = get_embedding(indicator_name)


indicator_has_index = {}
for index, value in enumerate(indicator_name, start=0):
    indicator_has_index[index] = {
        "index": index,
        'company_name': "大唐集团苏州分公司",
        "content": value,
        "embedding": content_embedding_list[index],
    }
es = Elasticsearch("http://localhost:9200", verify_certs=False)
index_name = "datang_docs_index3"

# 4.ES数据库中创建具有向量数据的索引
EMBEDDING_DIM = 1024  # bge-zh-1.5 输出维度

if not es.indices.exists(index=index_name):
    es.indices.create(
        index=index_name,
        body={
            "settings": {
                "number_of_shards": 1,
                "analysis": {
                    "analyzer": {
                        "ik_max_word_analyzer": {
                            "type": "custom",
                            "tokenizer": "ik_max_word"
                        }
                    }
                }
            },
            "mappings": {
                "properties": {
                    "company_name": {
                        "type": "text",
                        "analyzer": "ik_max_word",
                        "search_analyzer": "ik_smart"
                    },
                    "content": {
                        "type": "text",
                        "analyzer": "ik_max_word",
                        "search_analyzer": "ik_smart",
                        "fields": {
                            "keyword": {
                                "type": "keyword"
                            }
                        }
                    },
                    "index": {"type": "integer"},
                    "embedding": {
                        "type": "dense_vector",
                        "dims": EMBEDDING_DIM,
                        "index": True,
                        "similarity": "cosine"
                    }
                }
            }
        }
    )
    print("✅ 已创建支持向量和 IK 分词器的索引")
else:
    print("⚠️ 索引已存在，跳过创建")


# 5.ES数据库中存储文本数据和向量数据
actions = []
for item in indicator_has_index.values():
    company_name = str(item["company_name"])
    content = str(item["content"])
    embedding = item["embedding"]
    # full_text = title + " " + content
    # embedding = get_embedding(content)

    doc = {
        "company_name": company_name,
        "content": content,
        "index": int(item["index"]),
        "embedding": embedding
    }
    actions.append({"_index": index_name, "_source": doc})

bulk(es, actions)
print(f"✅ 成功写入 {len(actions)} 条文档到 Elasticsearch")
es.close()
