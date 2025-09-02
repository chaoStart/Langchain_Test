import os
import openai
from dotenv import load_dotenv
from elasticsearch import Elasticsearch
es = Elasticsearch("http://localhost:9200", verify_certs=False)
index_name = "datang_docs_index1"
load_dotenv()
os.environ["OPENAI_BASE_URL"] = "http://10.3.24.46:9997/v1"
os.environ["OPENAI_API_KEY"] = "123"

# 0.初始化嵌入模型对象
client = openai.Client()


def get_embedding(text):
    res = client.embeddings.create(input=text, model="bge-large-zh-v1.5")
    print("嵌入的查询文本:", text)
    return res.data[0].embedding


def search_company_name(query_text: str):
    """
    第一步：用 query_text 直接匹配 company_name
    不做 NER，直接用整个查询句子去检索公司名
    """
    resp = es.search(
        index=index_name,
        body={
            "query": {
                "match": {   # 用 match，不做分词精确控制
                    "company_name": query_text
                }
            },
            "_source": ["company_name"]
        }
    )
    hits = resp["hits"]["hits"]
    return [hit["_source"]["company_name"] for hit in hits]


def search_content(query_text: str, company_name: str, query_vector):
    """
    第三步：结合公司名硬过滤 + 内容关键词检索 + 向量打分
    """
    resp = es.search(
        index=index_name,
        body={
            "query": {
                "script_score": {
                    "query": {
                        "bool": {
                            "must": [   # 硬性过滤
                                {"term": {"company_name.keyword": company_name}}
                            ],
                            "should": [  # 用 IK 分词进行召回，不是硬条件
                                {"match": {"content": query_text}}
                            ]
                        }
                    },
                    "script": {
                        "source": "cosineSimilarity(params.query_vector, 'embedding') + 1.0",
                        "params": {
                            "query_vector": query_vector
                        }
                    }
                }
            },
            # "_source": ["company_name", "content"]
        }
    )
    return resp["hits"]["hits"]
# 用户输入问题
query_text = "大唐集团苏州分公司的是多少？"

# ===== Step 1：先尝试匹配公司名 =====
companies = search_company_name(query_text)
if not companies:
    print("未匹配到公司名称,使用默认公司名称作为过滤条件")
    # 这里假设默认公司是-大唐集团苏州分公司
    company_name = "大唐集团苏州分公司"
else:
    company_name = companies[0]  # 假设取第一个匹配到的公司
    # company_name = list(set(companies))
print(f"✅ 匹配到公司：{company_name}")
# ===== Step 2：获取文本嵌入向量数据 =====
query_vector = get_embedding(query_text)
# ===== Step 3：公司名过滤 + 内容检索 + 向量 rerank =====
results = search_content(query_text, company_name, query_vector)
# 过滤ES返回数据中的低分数据
def get_high_score(results_list):
    # 获取高分行召回数据
    record_high_score = []
    # 获取第一个得分最高最相关的数据
    score1 = results_list[0]["_score"]
    for index, item in enumerate(results_list):
        score_x = results_list[index]["_score"]
        if (score1 - score_x) / score1 <= 0.5:
            record_high_score.append(item)
    return record_high_score

res = get_high_score(results)

need_merged_company = []
for item in res:
    need_merged_company.append({
        item["_source"]["company_name"]: {"score": item["_score"],
                                          "dataset_name": item["_source"]["dataset_name"],
                                          "content": item["_source"]["content"]}})

# 组装成符合要求的数据
def same_company_content_merged(need_merge_company):
    results = {}
    for d in need_merge_company:
        for k, v in d.items():
            new_key = k+"|"+v["dataset_name"]
            if new_key not in results:
                results[k] = []
            results[k].append({v["dataset_name"]: v["content"]})

    print("相同公司下合并后的指标数据:\n", results)
    return results


res_merged = same_company_content_merged(need_merged_company)

# 打印结果
for k, v in res_merged.items():
    print(f" 公司名称: {k} - 日指标名称: {v}")
