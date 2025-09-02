import os
import openai
from dotenv import load_dotenv
from elasticsearch import Elasticsearch

es = Elasticsearch("http://localhost:9200", verify_certs=False)
index_name = "datang_docs_index1"
load_dotenv()
os.environ["OPENAI_BASE_URL"] = "http://10.3.24.46:9997/v1"
os.environ["OPENAI_API_KEY"] = "123"

print(es.indices.get_mapping(index=index_name))

# 0.初始化嵌入模型对象
client = openai.Client()


def get_embedding(text):
    res = client.embeddings.create(input=text, model="bge-large-zh-v1.5")
    print("嵌入的查询文本:", text)
    return res.data[0].embedding


def hybrid_search_indicator(query_text, query_vector, company_name):
    # ====== 1. 关键词检索 ======
    keyword_query = {
        "bool": {
            "must": [
                {"term": {"company_name.keyword": company_name}},  # 限定公司名称
            ],
            "should": [
                {"match": {
                    "content": {
                        "query": query_text,
                        "analyzer": "ik_max_word"
                    }
                }}
            ]
        }
    }
    keyword_res = es.search(index=index_name, query=keyword_query, size=10)["hits"]["hits"]

    # ====== 2. 向量检索 ======
    vector_query = {
        "script_score": {
            "query": {
                "bool": {
                    "must": [
                        {"term": {"company_name.keyword": company_name}},  # 精准匹配公司名
                        {"exists": {"field": "content"}}
                    ]
                }
            },
            "script": {
                "source": "(cosineSimilarity(params.query_vector, 'embedding') + 1.0)/2",
                "params": {"query_vector": query_vector}
            }
        }
    }

    vector_res = es.search(index=index_name, query=vector_query, size=10)["hits"]["hits"]

    # ====== 3. 合并逻辑 ======
    merged = {}
    # 先存放关键词检索结果
    for hit in keyword_res:
        key = (hit["_source"]["company_name"], hit["_source"]["dataset_id"], hit["_source"]["content"])
        merged[key] = {
            "source": hit["_source"],
            "bm25_score": hit["_score"],
            "vector_score": 0.0  # 暂无
        }

    # 再存放向量检索结果
    for hit in vector_res:
        key = (hit["_source"]["company_name"], hit["_source"]["dataset_id"], hit["_source"]["content"])
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

        final_results.append({
            "score": score,
            "company_name": val["source"]["company_name"],
            "dataset_id": val["source"]["dataset_id"],
            "content": val["source"]["content"]
        })

    # 排序
    final_results.sort(key=lambda x: x["score"], reverse=True)
    print("关键词+向量召回的指标数据总数:", len(final_results))
    # 取前10个
    top_10_results = final_results[:10]
    return top_10_results


# 用户输入问题
# query_text = "大唐集团南京分公司的滨海公司年完成率（小科）是多少？"
query_text = "公司名称:大唐江苏有限公司 时间:六月份 指标名称:泰州热电月度供电煤耗,吕四港公司年度供电煤耗 这两个指标的值是多少"
# query_text = "公司名称:大唐江苏有限公司 时间:六月份 指标名称:淮安光伏场站发电收益日指标（大屏）6月份的平均值?"

query_vector = get_embedding(query_text)

# 检索ES数据
results = hybrid_search_indicator(query_text, query_vector, company_name="大唐江苏有限公司")


# 过滤ES返回数据中的低分数据
def get_high_score(results_list):
    # 获取高分行召回数据
    record_high_score = []
    # 获取第一个得分最高最相关的数据
    score1 = results_list[0]["score"]
    for index, item in enumerate(results_list):
        score_x = results_list[index]["score"]
        if (score1 - score_x) / score1 <= 0.5:
            record_high_score.append(item)
    return record_high_score


high_indicator_res = get_high_score(results)

need_merged_company = []
for item in high_indicator_res:
    need_merged_company.append({
        item["company_name"]: {"score": item["score"],
                               "dataset_id": item["dataset_id"],
                               "content": item["content"]}})


# 组装成符合要求的数据
def same_company_content_merged(data: list[dict]) -> dict:
    results = {}
    for d in data:
        for company, v in d.items():
            dataset_id = v["dataset_id"]
            content = v["content"]

            if company not in results:
                results[company] = []

            # 查找是否已有该 dataset_id
            found = False
            for item in results[company]:
                if dataset_id in item:
                    item[dataset_id].append(content)
                    found = True
                    break

            if not found:
                results[company].append({dataset_id: [content]})

    return results


merged_res = same_company_content_merged(need_merged_company)

print("查询的指标数据:\n", merged_res, "\n")

# 打印结果
for k, v in merged_res.items():
    for each_item in v:
        for dt_id, dt_indicator in each_item.items():
            print(f" 公司名称: {k} - 数据集ID:{dt_id} - 对应的指标名称: {dt_indicator}")
