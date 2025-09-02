
import requests
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
es = Elasticsearch("http://localhost:9200", verify_certs=False)
index_name = "datang_docs_index1"
# from utils.embedded_model import get_embedding

# 1.设置请求路径和请求参数
url = "http://10.44.2.104:9090/mainApi/syncplant-business-dataset/api/empoworx/dataset/storeconfig/exec"

company_name = "大唐江苏有限公司"
# 请求数据（JSON 格式）
payload = {
    "code": "",
    "name": "",
    "companyName": company_name,
    "queryType": "0",
    "starttime": "2025-06-23 00:00:00",
    "endtime": "2025-06-23 23:59:59",
    "rowList": [],
    "datasetIds": [],
    "columnList": ["指标名称"],
    "rowPathList": [],
    "columnParam": {
        "指标结果": {
        },
        "指标名称": {
        }
    }
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

all_dataset_name = {}

# 3.对获取的数据进行预处理，包装为字典类型的指标数据——indicator_has_index
for k, v in response_data.items():
    indicator_name_list = []
    if len(v) == 0:
        continue
    else:
        current_item_all_name = v[0]["data"]["指标名称"]
        indicator_name_list.extend(current_item_all_name)
        all_dataset_name[k] = indicator_name_list

print(all_dataset_name)


# 3.1 初始化嵌入模型对象
import os
import openai
from dotenv import load_dotenv
load_dotenv()
os.environ["OPENAI_BASE_URL"] = "http://10.3.24.46:9997/v1"
os.environ["OPENAI_API_KEY"] = "123"

client = openai.Client()
def get_embedding(text_list):
    res = client.embeddings.create(input=text_list, model="bge-large-zh-v1.5")
    embeddings = [item.embedding for item in res.data]
    return embeddings


# 迭代存储不同dataset名称的指标名称

for dataset_name, item in all_dataset_name.items():

    # 对指标名称进行批量嵌入并获取嵌入结果
    indicator_name_embedding = get_embedding(item)

    indicator_has_index = {}
    for index, value in enumerate(item):
        indicator_has_index[index] = {
            "company_name": company_name,
            "dataset_name": dataset_name,
            "content": value,
            "embedding": indicator_name_embedding[index]
        }

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
                        "company_name":  {
                            "type": "text",
                            "analyzer": "ik_max_word",
                            "search_analyzer": "ik_smart",
                            "fields": {
                                "keyword": {
                                    "type": "keyword"
                                }
                            }
                        },
                        "dataset_id": {
                            "type": "text",
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

    #  5.ES数据库中存储文本数据和向量数据
    actions = []
    for item in indicator_has_index.values():
        dataId = ""
        if item["dataset_name"] == "智慧经营大屏1":
            dataId = "668430957309528820"
        elif item["dataset_name"] == "2025年日指标查询sql-新大唐南京":
            dataId = '642468644500176911'
        elif item["dataset_name"] == "2025年日指标查询sql-新南京热点储能":
            dataId = '642468644500176899'
        doc = {
            "company_name": str(item["company_name"]),
            "dataset_id": dataId,
            "content": str(item["content"]),
            "embedding": item["embedding"]
        }
        actions.append({"_index": index_name, "_source": doc})

    bulk(es, actions)
    print(f"✅ 成功写入 {len(actions)} 条文档到 Elasticsearch")

