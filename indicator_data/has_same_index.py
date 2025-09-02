import pandas as pd

# 读取Excel文件，指定sheet_name为'sheet1'
df = pd.read_excel('2025年大唐江苏公司.xlsx', sheet_name='619日指标', engine='openpyxl')

# 读取第2列数据（索引为1）
second_col = df.iloc[:, 1]

# 判断是否有重复数据
duplicates = second_col.duplicated()

if duplicates.any():
    print("第2列存在重复数据。重复的数据有：")
    # 获取重复的数据项（包括第一次出现的）
    duplicate_values = second_col[second_col.duplicated(keep=False)]
    print(duplicate_values.unique())
else:
    print("第2列不存在重复数据。")
