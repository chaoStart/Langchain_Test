import langchain
from langchain_openai import ChatOpenAI
import os
import getpass
os.environ["LANGCHAIN_TRACING_V2"] = "true"
os.environ["LANGCHAIN_PROJECT"] = "pr-jaunty-revival-1"
os.environ["LANGCHAIN_ENDPOINT"] = "https://api.smith.langchain.com"
os.environ["LANGCHAIN_API_KEY"] = "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"
os.environ["OPENAI_API_KEY"] = "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"

llm = ChatOpenAI(openai_api_key="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")
# llm = ChatOpenAI()
# llm.invoke("Hello, world!")
llm.invoke("介绍一下李白？")

if __name__ == '__main__':
    print(langchain.__version__)