
import  os
import pandas as pd
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

# 读取 Excel 文件（指定 engine 为 openpyxl）
# df = pd.read_excel('2025年日指标大唐江苏公司.xlsx', engine='openpyxl')
Sheet_name = '619日指标'
df = pd.read_excel('2025年大唐江苏公司.xlsx', sheet_name=Sheet_name, engine='openpyxl', header=None)

# 获取前 5 行数据
first_5_rows = df.head(5)

# 输出结果
print(first_5_rows)
print("--------------------------------------")

# 替换第一列为从 0 行开始的递增整数
df.iloc[:, 0] = range(1, len(df) + 1)
df.columns = ['序号', '指标名称', '数值', '单位']
# 查看前几行确认是否生效
print(df.head())
print("-------------------------------------")
for row in df.itertuples(index=False):
    print(row)
# 2. 删除第2列中重复的值（只保留第一次出现的）
# df_unique = df.drop_duplicates(subset=df.columns[1], keep='first')
#
# #  如果文件夹中存在“处理后的文件.xlsx”，则跳过导出文件步骤
# if os.path.exists('处理后的文件.xlsx'):
#     # 3. 导出处理后的数据为新的 Excel 文件
#     print("文件存在")
# else:
#     print("已经导出处理后的文件.xlsx")
#     df_unique.to_excel('处理后的文件.xlsx', index=False)
#
# print("查看数据[df.columns[1]]:",df.columns[1])