
import requests
from storage_dataset import  storage_financial_data2_es

# 请求地址
url = "http://10.44.2.104:9090/mainApi/syncplant-business-dataset/api/empoworx/dataset/storeconfig/process"

# 请求数据（JSON 格式）
payload = {
    "code":"",
    "name":"",
    "companyName":"大唐集团苏州分公司",
    "datasetIds":[],
    "queryType":"1",
    "starttime":"2025-07-16 00:00:00",
    "endtime":"2025-07-16 23:59:59",
    "rowList":[],
    "columnList":[],
    "rowPathList":[],
    "columnParam":{}
}

# 设置请求头（Content-Type 为 application/json）
headers = {
    "Content-Type": "application/json"
}

def fetch_data(url, payload, headers):
    try:
        response = requests.post(url, json=payload, headers=headers)
        response.raise_for_status()  # 检查HTTP请求是否成功
        return response.json()
    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error occurred: {http_err}")
    except Exception as err:
        print(f"Other error occurred: {err}")

# 获取初始数据
data = fetch_data(url, payload, headers)
if not data:
    exit()

def recursion_row_chidren_all(item,name_list):
    if len(item["children"]) != 0:
        name_list.append(item["value"])
        for c in item["children"]:
            recursion_row_chidren_all(c, name_list)
    else:
        name_list.append(item["value"])
    # 获取所有的sheet工作表数据，即.xlsx文件中所有的工作表数据集。all_sheet_name是一个list类型数据
all_sheet_data = data["data"]

# 创建一个list数组，用来存储每一个sheet工作表的行列名称
all_sheet_list = []
sheet_dict = {}
for each_name in all_sheet_data:
    one_sheet_data = all_sheet_data[each_name]
    company_name = one_sheet_data["编制单位"]
    data_month = one_sheet_data["数据编制时间"]
    sheet_name = one_sheet_data["sheet名称"]
    table_name = one_sheet_data["标题名称"]
    # 获取的column_name是List类型的数组
    column_name = one_sheet_data["columnList"]
    big_column_name = " ".join(column_name)
    print("合并后的总列表名称:",big_column_name)
    # 获取的行数据是一个list列表类型的数据，需要进一步转换为整个行数据
    # row_list_data = one_sheet_data["data"]
    if len(one_sheet_data["data"]) != 0:
         row_list_data = one_sheet_data["data"][0]["data"]
    else:
        continue
    if len(row_list_data) != 0:
        row_name = []
        for item in row_list_data:
            name_list = []  # 大字段存储集和
            # 直接把所有行名称拼接成为一个大字段
            recursion_row_chidren_all(item,name_list)
            current_node_all_row_name = "\n".join(name_list)
            row_name.append(current_node_all_row_name)
            # 对具有各个子项目的行进行合并，成为一个大的行
            big_row_name="\n".join(row_name)

        # print("合并后的全行名称:", big_row_name)
        sheet_dict = {
        "company_name": company_name,
        "data_month": data_month,
        "sheet_name": sheet_name,
        "table_name": table_name,
        "row_name": big_row_name,
        "column_name": big_column_name
        }
        all_sheet_list.append(sheet_dict)
    else:
        continue
print("🧠成功获取数据集的sheet和对应的整行整列名称")
storage_financial_data2_es(all_sheet_list)

