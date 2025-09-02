import os
import pandas as pd
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

# 1. 读取 Excel
Sheet_name = '2025年日指标查询sql-新'
df = pd.read_excel('2025年日指标大唐江苏公司.xlsx', sheet_name=Sheet_name, engine='openpyxl')

# 2. 去重：以标题列为基础去重
df_unique = df.drop_duplicates(subset=df.columns[1], keep='first')

# 2.建立索引：替换日期，依次递增为每一个指标名称建立一个索引id
df_unique.iloc[:, 0] = range(1, len(df_unique) + 1)
# df_unique.columns = ['序号', '指标名称', '数值', '单位']

# 查看前几行确认是否生效
print(df_unique.head())

# 3. 导出去重后的文件（可选）
if not os.path.exists('处理后的文件.xlsx'):
    df_unique.to_excel('处理后的文件.xlsx', index=False)
    print("已导出处理后的文件.xlsx")
else:
    print("文件已存在，跳过导出")

# 4. 提取标题列（第1列）和内容列（第3列）
df_docs = df_unique[[df.columns[3], df.columns[1], df.columns[0]]].rename(
    columns={
        df.columns[0]: "index",
        df.columns[1]: "content",
        df.columns[3]: "title"
    }
)
print(df_docs.head(5))
# 5. 连接 Elasticsearch
es = Elasticsearch("http://localhost:9200", verify_certs=False)
index_name = 'datang_docs_index1'

# 6. 创建支持 multi_match 的索引（含 ik 分词器）
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

# 7. 批量构造写入数据
actions = []
for _, row in df_docs.iterrows():
    doc = {
        "title": str(row["title"]) if pd.notna(row["title"]) else "",
        "content": str(row["content"]) if pd.notna(row["content"]) else "",
        "index": int(row["index"]) if pd.notna(row["index"]) else -1
    }
    actions.append({"_index": index_name, "_source": doc})

bulk(es, actions)
print(f"✅ 成功写入 {len(actions)} 条文档到 Elasticsearch 索引：{index_name}")

es.close()
