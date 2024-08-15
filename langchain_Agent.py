import langchain
# from langchain_openai import ChatOpenAI
import os
from langchain.agents import create_react_agent
from langchain_community.document_loaders import WebBaseLoader
from langchain_community.llms import Ollama
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import StrOutputParser
import langsmith
# llm = Ollama(model="llama2")
llm = Ollama(model="qwen2")

print('-------------下面加载本地PDF文档，进行问答测试；-----------------------')
from langchain_community.document_loaders import PyPDFLoader

loader = PyPDFLoader("./paper2_1.pdf")
PDF_data = loader.load_and_split()
print('PDF的pages是什么：', PDF_data[2])
# 接下来，我们需要将其索引到向量存储中。这需要几个组件，即嵌入模型和向量存储。

from langchain_community.embeddings import ModelScopeEmbeddings

model_id = "damo/nlp_corom_sentence-embedding_english-base"
embeddings = ModelScopeEmbeddings(model_id=model_id, model_revision="v1.0.0")

# 使用此嵌入模型将文档摄取到向量存储中。 我们将使用一个简单的本地向量库 FAISS。
from langchain_community.vectorstores import FAISS
from langchain_text_splitters import RecursiveCharacterTextSplitter

# 加载本地PDF的信息进行问答
text_splitter = RecursiveCharacterTextSplitter()
text_documents = text_splitter.split_documents(PDF_data)
text_vector = FAISS.from_documents(text_documents, embeddings)
retriever = text_vector.as_retriever()

from langchain.tools.retriever import create_retriever_tool

retriever_tool = create_retriever_tool(
    retriever,
    "rzdf_search",
    "搜索有关论文《航空制造数控加工刀具健康监测方法研究》的信息。对于任何关于《航空制造数控加工刀具健康监测方法研究》的问题，你必须使用此工具！",
)

from langchain.agents import load_tools

tools = load_tools(['llm-math'], llm=llm)
tools.append(retriever_tool)

from langchain import hub
from langchain.agents import create_react_agent
from langchain.agents import AgentExecutor

# 这里获取一个内置prompt，为了方便
prompt = hub.pull("hwchase17/react")
agent = create_react_agent(llm, tools, prompt)
agent_executor = AgentExecutor(agent=agent, tools=tools, verbose=True)
print(agent_executor.invoke({"input": "论文《航空制造数控加工刀具健康监测方法研究》的作者是谁？"}))
print('-----------8 的立方根是多少？乘以 13.27，然后减去 5-----------------')
print(agent_executor.invoke({"input": "8 的立方根是多少？乘以 13.27，然后减去5"}))
