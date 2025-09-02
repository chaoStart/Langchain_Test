import os
import pandas as pd
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

# 1. 读取 Excel
Sheet_name = '2025年日指标查询sql-新'
df = pd.read_excel('2025年日指标大唐江苏公司.xlsx', sheet_name=Sheet_name, engine='openpyxl')

# 获取前几行数据看看
print(df.iloc[:5, 1:2])
print("--------------------------------------")

# 2. 去重
df_unique = df.drop_duplicates(subset=df.columns[1], keep='first')

# 导出（如果不存在）
if not os.path.exists('处理后的文件.xlsx'):
    df_unique.to_excel('处理后的文件.xlsx', index=False)
    print("已导出处理后的文件.xlsx")
else:
    print("文件已存在，跳过导出")

# 3. 提取用于写入的数据
df_column = df_unique[[df.columns[1]]].rename(columns={df.columns[1]: 'value'})

# 4. 连接 Elasticsearch
es = Elasticsearch("http://localhost:9200", verify_certs=False)
index_name = 'datang_index_name'

# 5. 创建索引（如果不存在），并指定 value 字段使用 IK 分词器
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
                    "value": {
                        "type": "text",
                        "analyzer": "ik_max_word",      # 索引时使用 ik_max_word
                        "search_analyzer": "ik_smart"   # 查询时可用 ik_smart 提高精度
                    }
                }
            }
        }
    )
    print("索引已创建，启用 IK 分词器")
else:
    print("索引已存在，跳过创建")

# 6. 构造 bulk 数据并写入
actions = [
    {
        "_index": index_name,
        "_source": {
            "value": row["value"]
        }
    }
    for _, row in df_column.iterrows()
]

bulk(es, actions)

print(f"✅ 成功写入 {len(actions)} 条数据到 Elasticsearch 索引：{index_name}")
