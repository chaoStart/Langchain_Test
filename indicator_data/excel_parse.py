
import  os
import pandas as pd
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

# 读取 Excel 文件（指定 engine 为 openpyxl）
# df = pd.read_excel('2025年日指标大唐江苏公司.xlsx', engine='openpyxl')
Sheet_name = '2025年日指标查询sql-新'
df = pd.read_excel('2025年日指标大唐江苏公司.xlsx', sheet_name=Sheet_name, engine='openpyxl')

# 获取第一列的前10行
first_10_rows = df.iloc[:5, 1:2]

# 输出结果
print(first_10_rows)
print("--------------------------------------")
# 2. 删除第2列中重复的值（只保留第一次出现的）
df_unique = df.drop_duplicates(subset=df.columns[1], keep='first')

#  如果文件夹中存在“处理后的文件.xlsx”，则跳过导出文件步骤
if os.path.exists('处理后的文件.xlsx'):
    # 3. 导出处理后的数据为新的 Excel 文件
    print("文件存在")
else:
    print("已经导出处理后的文件.xlsx")
    df_unique.to_excel('处理后的文件.xlsx', index=False)

print("查看数据[df.columns[1]]:",df.columns[1])
df_column = df_unique[[df.columns[1]]].rename(columns={df.columns[1]: 'value', df.columns[3]: Sheet_name})

# ---------------------- 2. 连接 Elasticsearch ----------------------
es = Elasticsearch("http://localhost:9200",verify_certs=False)  # 本地的ES地址

# ---------------------- 3. 创建索引（如不存在） ----------------------
# 测试连接和索引
try:
    index_name = 'datang_index_name'  # 替换为你希望的索引名称
    if not es.indices.exists(index=index_name):
        es.indices.create(index=index_name)
    print("连接成功，索引检查通过")
except Exception as e:
    print("连接失败，错误信息：", e)

# ---------------------- 4. 批量写入数据 ----------------------
# 构建 bulk 操作的数据格式
actions = [
    {
        "_index": index_name,
        "_source": {
            "value": row["value"]
        }
    }
    for _, row in df_column.iterrows()
]

# 写入
bulk(es, actions)

print(f"成功写入 {len(actions)} 条数据到 Elasticsearch 索引：{index_name}")