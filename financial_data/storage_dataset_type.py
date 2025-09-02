
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

# 连接 Elasticsearch
es = Elasticsearch("http://localhost:9200", verify_certs=False)
index_name = 'suzhou_storage_dataset_big_chunk1'
EMBEDDING_DIM = 1024
def new_clear_es(index_name):
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
                    "company_name": {"type": "keyword"},  # 或 {"type": "keyword", "index": false} 若从不用于过滤
                    "sheet_type": {"type": "keyword"},  # 同上
                    #"data_month": {"type": "keyword"},
                    # 若需要范围 / 排序，另加
                    "data_month": {"type": "date", "format": "yyyy-MM"},
                    # "table_name": {"type": "keyword"},  # 同上

                    # "company_name": {
                    #     "type": "text",
                    #     "analyzer": "ik_max_word",
                    #     "search_analyzer": "ik_smart",
                    #     "fields": {
                    #         "keyword": {
                    #             "type": "keyword"
                    #         }
                    #     }
                    # },
                    # "sheet_type": {
                    #     "type": "text",
                    #     "analyzer": "ik_max_word",
                    #     "search_analyzer": "ik_smart"
                    # },
                    # "data_month": {
                    #     "type": "text",
                    #     "analyzer": "ik_max_word",
                    #     "search_analyzer": "ik_smart",
                    #     "fields": {
                    #         "keyword": {
                    #             "type": "keyword"
                    #         }
                    #     }
                    # },
                    "sheet_name": {
                        "type": "text",
                        "analyzer": "ik_max_word",
                        "search_analyzer": "ik_smart",
                        "fields": {
                            "keyword": {
                                "type": "keyword"
                            }
                        }
                    },
                    "table_name": {
                        "type": "text",
                        "analyzer": "ik_max_word",
                        "search_analyzer": "ik_smart",
                        "fields": {
                            "keyword": {
                                "type": "keyword"
                            }
                        }
                    },
                    "row_name": {
                        "type": "text",
                        "analyzer": "ik_max_word",
                        "search_analyzer": "ik_smart"
                    },
                    "column_name": {
                        "type": "text",
                        "analyzer": "ik_max_word",
                        "search_analyzer": "ik_smart"
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
    print("✅ 已创建索引并启用 IK 分词器")

# 创建存储数据的方法，传入公司名称+日期+工作表sheet+行名+列名

import re

def normalize_month(month_str):
    """
    把 '2025年1月' 转换成 '2025-01'
    """
    match = re.match(r'(\d{4})年(\d{1,2})月', month_str)
    if match:
        year, month = match.groups()
        return f"{year}-{int(month):02d}"  # 补齐两位
    return month_str  # 如果格式不对，原样返回


def storage_financial_data2_es(sheet_dict):
    #  创建支持 multi_match 的索引（含 ik 分词器）
    if not es.indices.exists(index=index_name):
        new_clear_es(index_name)
    else:
        # 删除已有索引（谨慎操作）
        print("⚠️ 索引已存在！！！")

    # 批量构造写入数据
    actions = []
    for item in sheet_dict:
        # 校验数据类型。判断是否包含 'row_name' 且其值不为 None 或空
        if 'row_name' in item and item['row_name'] is not None:
            key_type = 'row_name'
            row_or_column = str(item['row_name'])
        else:
            key_type = 'column_name'
            row_or_column = str(item['column_name'])
        doc = {
            "company_name": str(item["company_name"]),
            "data_month": normalize_month(str(item["data_month"])),
            "sheet_name": str(item["sheet_name"]),
            "table_name": str(item["table_name"]),
            "sheet_type": str(item["sheet_type"]),
            "embedding": item["embedding"],
            key_type: row_or_column
        }
        actions.append({"_index": index_name, "_source": doc})

    bulk(es, actions)
    print(f"✅ 成功写入 {len(actions)} 条文档到 Elasticsearch 索引：{index_name}")