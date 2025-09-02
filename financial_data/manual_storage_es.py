
import pandas as pd
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

# 1. 连接 Elasticsearch
es = Elasticsearch("http://localhost:9200", verify_certs=False)
index_name = 'manual_storage_es_big_chunk'

# 2.读取自己编写的.xlsx文件，并存入到es数据库
# df_docs =pd.read_excel('2025年日指标大唐江苏公司.xlsx', sheet_name=Sheet_name, engine='openpyxl')
df_docs =pd.read_excel('手动创建的存储数据_整块.xlsx', engine='openpyxl')
print(df_docs.head(5))

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
                    "company_name": {
                        "type": "text",
                        "analyzer": "ik_max_word",
                        "search_analyzer": "ik_smart"
                    },
                    "data_month": {
                        "type": "text",
                        "analyzer": "ik_max_word",
                        "search_analyzer": "ik_smart"
                    },
                    "sheet_name": {
                        "type": "text",
                        "analyzer": "ik_max_word",
                        "search_analyzer": "ik_smart"
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
                    # "content": {
                    #     "type": "text",
                    #     "analyzer": "ik_max_word",
                    #     "search_analyzer": "ik_smart"
                    # }
                }
            }
        }
    )
    print("✅ 已创建索引并启用 IK 分词器")
    # 4. 批量构造写入数据
    actions = []
    for _, row in df_docs.iterrows():
        doc = {
            "company_name": str(row["Company_name"]) if pd.notna(row["Company_name"]) else "",
            "data_month": str(row["Data_month"]) if pd.notna(row["Data_month"]) else "",
            "sheet_name": str(row["Sheet_name"]) if pd.notna(row["Sheet_name"]) else "",
            "row_name": str(row["Row_name"]) if pd.notna(row["Row_name"]) else "",
            "column_name": str(row["Column_name"]) if pd.notna(row["Column_name"]) else "",
            # "content": str(row["Content"]) if pd.notna(row["Content"]) else ""
        }
        actions.append({"_index": index_name, "_source": doc})

    bulk(es, actions)
    print(f"✅ 成功写入 {len(actions)} 条文档到 Elasticsearch 索引：{index_name}")

# 3. 创建支持 multi_match 的索引（含 ik 分词器）
if not es.indices.exists(index=index_name):
    new_clear_es(index_name)
else:
    # 删除已有索引（谨慎操作）
    print("⚠️ 索引已存在！！！")
    # es.indices.delete(index=index_name)
    # new_clear_es(index_name)


# 5.简单测试是否可以查询检索
response = es.search(index=index_name, body={
    "query": {
        "match": {
            "column_name": "平均发电设备容量和本月数",
        }
    },
    "_source": ["sheet_name"],  # 只返回 sheet_name 字段
    "size": 15,
})
sheet_names = [hit["_source"]["sheet_name"] for hit in response["hits"]["hits"]]
print("✅ 根据列名返回查询的检索信息",sheet_names)


# 6. 使用 multi_match 查询 row_name、column_name 和 content 字段，返回匹配的 sheet_name
def search_sheet_name_by_keywords(keywords, top_k=5):
    query_body = {
        "query": {
            "multi_match": {
                "query": keywords,
                # "fields": ["row_name", "column_name", "content"],
                "fields": ["row_name", "column_name"],
                # "type": "most_fields",  # 可以改为 "most_fields" 或 "phrase" 视具体效果调整
                "type": "cross_fields",  # 可以改为 "most_fields" 或 "phrase" 视具体效果调整
                "analyzer": "ik_max_word"
            }
        },
        "_source": ["sheet_name"],  # 只返回 sheet_name 字段
        "size": top_k
    }

    response = es.search(index=index_name, body=query_body)
    sheet_names = [hit["_source"]["sheet_name"] for hit in response["hits"]["hits"]]
    return sheet_names

# 示例调用：根据关键词查找工作表名称
question = "发电厂用电量的本年累计数"
matched_sheets = search_sheet_name_by_keywords(question)
print("🔍 根据行名+列名 匹配的工作表名称：", matched_sheets)
print("------------------------------------------------")
# 验证当前索引中是否有文档
print("当前索引文档总数：", es.count(index=index_name)["count"])