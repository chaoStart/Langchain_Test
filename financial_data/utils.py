from elasticsearch import Elasticsearch

es = Elasticsearch("http://localhost:9200", verify_certs=False)
index_name = "suzhou_storage_dataset_big_chunk1"


def query_by_column_text_embedded(query_text, query_vector, filter_conditions):
    # ====== 1. 关键词检索 ======
    keyword_query = {
        "bool": {
            "must": [
                {"term": {"company_name": filter_conditions["company_name"]}},
                {"term": {"table_name.keyword": filter_conditions["table_name"]}},
                {"term": {"data_month": filter_conditions["data_month"]}},
                {"exists": {"field": "column_name"}}  # column_name 必须存在
            ],
            "should": [
                {
                    "match": {
                        "column_name": {
                            "query": query_text,
                            "analyzer": "ik_smart"
                        }
                    }
                }
            ],
            "minimum_should_match": 1
        }
    }
    keyword_res = es.search(index=index_name, query=keyword_query, size=10)["hits"]["hits"]

    if len(keyword_res) == 0:
        return []

    # ====== 2. 向量检索 ======
    vector_query = {
        "script_score": {
            "query": {
                "bool": {
                    "must": [
                        {"term": {"company_name": filter_conditions["company_name"]}},
                        {"term": {"table_name.keyword": filter_conditions["table_name"]}},
                        {"term": {"data_month": filter_conditions["data_month"]}},
                        {"exists": {"field": "column_name"}}  # column_name 必须存在
                    ]
                }
            },
            "script": {
                "source": "cosineSimilarity(params.query_vector, 'embedding') + 1.0",
                "params": {"query_vector": query_vector}
            }
        }
    }
    vector_res = es.search(index=index_name, query=vector_query, size=10)["hits"]["hits"]

    # ====== 3. 合并逻辑 ======
    merged = {}

    # 先放关键词结果
    for hit in keyword_res:
        key = (hit["_source"]["table_name"], hit["_source"]["column_name"])
        merged[key] = {
            "source": hit["_source"],
            "bm25_score": hit["_score"],
            "vector_score": 0.0
        }

    # 再放向量结果
    for hit in vector_res:
        key = (hit["_source"]["table_name"], hit["_source"]["column_name"])
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
            "company_name": val["source"].get("company_name", ""),
            "table_name": val["source"].get("table_name", ""),
            "data_month": val["source"].get("data_month", ""),
            "sheet_type": val["source"].get("sheet_type", ""),
            "row_name": val["source"].get("row_name", ""),
            "column_name": val["source"].get("column_name", "")
        })

    # 排序
    final_results.sort(key=lambda x: x["score"], reverse=True)
    return final_results


def query_by_row_text_embedded(query_text, query_vector):
    # ====== 1. 关键词检索 ======
    keyword_query = {
        "bool": {
            "must": [  # 必须包含 row_name 字段
                {"exists": {"field": "row_name"}}
            ],
            # "should": [
            #     {
            #         "match": {
            #             "row_name": {
            #                 "query": query_text,
            #                 "boost": 1,
            #                 "analyzer": "ik_smart"
            #             }
            #         }
            #     },
            #     {
            #         "match": {
            #             "table_name": {
            #                 "query": query_text,
            #                 "boost": 2,
            #                 "analyzer": "ik_smart"
            #             }
            #         }
            #     }
            # ],
            "should": [
                {
                    "multi_match": {
                        "query": query_text,
                        "type": "most_fields",  # 默认类型，取最得分和
                        "fields": ["row_name^1", "table_name^2"],  # ^ 1表示boost = 1， ^ 2表示boost = 2
                        "analyzer": "ik_max_word"
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
                        {"exists": {"field": "row_name"}}  # 仅当 row_name 存在
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
        key = (hit["_source"]["table_name"],
               hit["_source"]["row_name"] if "row_name" in hit["_source"] else hit["_source"].get("column_name", ""))
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
            "company_name": val["source"]["company_name"],
            "table_name": val["source"]["table_name"],
            "sheet_name": val["source"]["sheet_name"],
            "data_month": val["source"]["data_month"],
            "sheet_type": val["source"].get("sheet_type", ""),
            row_or_column: content
        })

    # 排序
    final_results.sort(key=lambda x: x["score"], reverse=True)
    print("行关键词+向量召回的数据记录合计总数:", len(final_results))
    # 取前10个
    top_10_results = final_results[:10]

    # 输出
    for r in top_10_results:
        content = r["row_name"] if "row_name" in r else r["column_name"]
        print(f"[{r['score']:.2f}] {r['table_name']} - 数据类型:{r['sheet_type']} - {content}\n\n")

    return top_10_results


def get_all_column_name(filter_conditions):
    # Step 1: 按条件检索
    query = {
        "bool": {
            "must": [
                {"term": {"company_name": filter_conditions["company_name"]}},
                {"term": {"table_name.keyword": filter_conditions["table_name"]}},
                {"term": {"data_month": filter_conditions["data_month"]}},
                {"exists": {"field": "column_name"}}  # 仅当 column_name 存在
            ]
        }
    }
    response = es.search(index=index_name, query=query, size=1000)

    # 把满足条件的数据保存到 list
    hits = response["hits"]["hits"]
    results = [hit["_source"] for hit in hits]

    # Step 2: 遍历，按 table_name 聚合 column_name
    from collections import defaultdict

    table_columns = defaultdict(list)

    for doc in results:
        table_name = doc["table_name"]
        # column_name = doc.get("column_name", "")
        if "column_name" in doc and doc["column_name"]:
            column_name = doc["column_name"]
            table_columns[table_name].append(column_name)

    # 将相同 table_name 的 column_name 拼接为字符串（换行符分隔）
    # table_columns_str = {t: "\n".join(cols) for t, cols in table_columns.items()}
    # columns_str = "\n".join(
    #     f"{t}\n{cols}" for t, cols in table_columns_str.items()
    # )
    # print("按表名聚合后的列名：", table_columns_str)
    # columns_str = '\n'.join(columns_str.splitlines()[1:])
    # return columns_str
    for sheet_name, all_column in table_columns.items():
        return all_column


def get_sheet_name_unit(filter_conditions):
    # Step 1: 按条件检索
    query = {
        "bool": {
            "must": [
                {"term": {"company_name": filter_conditions["company_name"]}},
                {"term": {"table_name.keyword": filter_conditions["table_name"]}},
                {"term": {"data_month": filter_conditions["data_month"]}},
                {"exists": {"field": "column_name"}}
            ]
        }
    }
    response = es.search(index=index_name, query=query, size=50)

    # 把满足条件的数据保存到 list
    hits = response["hits"]["hits"]
    results = [hit["_source"] for hit in hits]
    for doc in results:
        if doc["column_name"] == "单位":
            return "单位"

    return 0
