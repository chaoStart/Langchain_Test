
import os
import openai
from dotenv import load_dotenv
load_dotenv()
os.environ["OPENAI_BASE_URL"] = "http://10.3.24.46:9997/v1"
os.environ["OPENAI_API_KEY"] = "123"
client = openai.Client()

res = client.embeddings.create(input=["我是中国人"],model="bge-large-zh-v1.5")

print(res)
print(res.data[0].embedding)