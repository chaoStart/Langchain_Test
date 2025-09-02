import  os
import pandas as pd
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk


# 1、 读取 Excel 文件，遍历文件具有哪些工作表sheet
file_path = '../江苏大唐国际吕四港发电有限责任公司.xlsx'

# 使用 pandas.ExcelFile 获取所有 sheet 名称
with pd.ExcelFile(file_path) as xls:
    sheet_names = xls.sheet_names

# 打印所有 sheet
print(f"该文件共有 {len(sheet_names)} 个 sheet 工作表。工作表name存在{type(sheet_names)}格式中")

# 遍历并打印每个 sheet 名称
for i, name in enumerate(sheet_names, 1):
    print(f"{i}. {name}")

print("--------------------------------------")

# 2、读取 Excel 文件中具体的某一张工作表sheet，如指定为CWYB2406 利润明细表 （指定 engine 为 openpyxl）
Sheet_name = 'CWYB2401 大唐集团参数表'
df = pd.read_excel('江苏大唐国际吕四港发电有限责任公司.xlsx', sheet_name=Sheet_name, engine='openpyxl', header=None)

# 获取前 5 行数据
first_5_rows = df.head(5)

# 输出结果
print(first_5_rows)
print("--------------------------------------")