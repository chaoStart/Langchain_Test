import os
import openai
from dotenv import load_dotenv
load_dotenv()
os.environ["OPENAI_BASE_URL"] = "http://10.3.24.46:9997/v1"
os.environ["OPENAI_API_KEY"] = "123"

# 0.初始化嵌入模型对象
client = openai.Client()

def get_embedding(text):
    res = client.embeddings.create(input=text, model="bge-large-zh-v1.5")
    print("触发N次？",res.data[0].embedding)
    return res.data[0].embedding