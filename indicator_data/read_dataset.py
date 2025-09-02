
import requests
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

# 1.请求外部数据集地址
url = "http://10.44.2.104:9090/mainApi/syncplant-business-dataset/api/empoworx/dataset/storeconfig/exec"

# 请求数据（JSON 格式）
payload = {
    "code":"",
    "name":"",
    "companyName":"大唐集团南京分公司",
    "queryType":"0",
    "starttime":"2025-07-23 00:00:00",
    "endtime":"2025-07-23 23:59:59",
    "rowList":[],
    "datasetIds":[],
    "columnList":["指标名称"],
    "rowPathList":[],
    "columnParam":{
        "指标结果":{
        },
        "指标名称":{
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


# 1.请求外部数据集地址，获取初始数据
data = fetch_data(url, payload, headers)
if not data:
    exit()

response_data = data["data"]

indicator_name = []

# 2.对获取的数据进行预处理，包装为字典类型的数据
for k,v in response_data.items():
    current_item_all_name = v["data"]["指标名称"]
    indicator_name.extend(current_item_all_name)

print(indicator_name)

indicator_has_index = {}
for index, value in enumerate(indicator_name, start=1):
    indicator_has_index[index] = {
        "index": index,
        'title': "江苏大唐日指标",
        "content": value
    }

print(indicator_has_index)

# 3. 连接 Elasticsearch
es = Elasticsearch("http://localhost:9200", verify_certs=False)
index_name = 'datang_docs_index1'

# 4. 创建支持 multi_match 的索引（含 ik 分词器）
if not es.indices.exists(index=index_name):
    es.indices.create(
        index=index_name,
        body={
            "settings": {
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
                    "title": {
                        "type": "text",
                        "analyzer": "ik_max_word",
                        "search_analyzer": "ik_smart"
                    },
                    "content": {
                        "type": "text",
                        "analyzer": "ik_max_word",
                        "search_analyzer": "ik_smart"
                    },
                    "index": {
                        "type": "integer"
                    }
                }
            }
        }
    )
    print("✅ 已创建索引并启用 IK 分词器")
else:
    print("⚠️ 索引已存在，跳过创建")

# 5. 批量构造写入数据
actions = []
for item in indicator_has_index.values():
    doc = {
        "INDEX": str(item["index"]),
        "COMPANY_TITLE": str(item["title"]),
        "INDICATOR_NAME": str(item["content"])
    }
    actions.append({"_index": index_name, "_source": doc})

bulk(es, actions)
print(f"✅ 成功写入 {len(actions)} 条文档到 Elasticsearch 索引：{index_name}")

es.close()