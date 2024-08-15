import langchain
from langchain_openai import ChatOpenAI
from langchain_community.llms import Ollama
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import StrOutputParser
from langchain_community.document_loaders import TextLoader

loader = TextLoader("example_data/layout-parser-paper.txt")

data = loader.load()

llm = Ollama(model="llama2")
import os

# 测试llama2模型是否启用
# llm.invoke("Hello, world!")
print(llm.invoke("你好？"))
print('--------------------')
# 设置提升模版
prompt = ChatPromptTemplate.from_messages([
    ("system", "You are a world class technical documentation writer."),
    ("user", "{input}")
])
# 结合提升模版进行测试输出
chain = prompt | llm
print(chain.invoke({"input": "你好？"}))
print('------------------------------')
# 添加一个简单的输出解析器，将聊天消息转换为字符串。
output_parser = StrOutputParser()
chain = prompt | llm | output_parser
print(chain.invoke({"input": "Where is the capital of China？"}))
print('-------------------------------')

# 使用检索链 Retrieve
from langchain.chains.combine_documents import create_stuff_documents_chain
prompt = ChatPromptTemplate.from_template("""仅根据提供的上下文回答以下问题:
<上下文>
{context}
</上下文>
问题: {input}""")
document_chain = create_stuff_documents_chain(llm, prompt)
from langchain_core.documents import Document

print(document_chain.invoke({
    "input": "What sports does Bian Qingchao like？",
    "context": [Document(
        page_content="Bian Qingchao is a new employee of the clerk company. He is a recent graduate student from Nanjing, Jiangsu and enjoys playing basketball")]
}))

print('------------------------------------')
print('-------------下面加载Langchain的官方使用手册，进行问答测试-----------------------')
#加载Langchain的文档数据
from langchain_community.document_loaders import WebBaseLoader
loader = WebBaseLoader("https://docs.smith.langchain.com/user_guide")
docs = loader.load()

# 接下来，我们需要将其索引到向量存储中。这需要几个组件，即嵌入模型和向量存储。
from langchain_community.embeddings import OllamaEmbeddings
embeddings = OllamaEmbeddings()

# 使用此嵌入模型将文档摄取到向量存储中。 我们将使用一个简单的本地向量库 FAISS。
from langchain_community.vectorstores import FAISS
from langchain_text_splitters import RecursiveCharacterTextSplitter

text_splitter = RecursiveCharacterTextSplitter()
documents = text_splitter.split_documents(docs)
vector = FAISS.from_documents(documents, embeddings)
'''
现在我们已经在向量存储中索引了这些数据，我们将创建一个检索链。 
该链将接受一个传入的问题，查找相关文档，然后将这些文档与原始问题一起传递给 LLM，并要求它回答原始问题。
'''
# 首先，让我们设置一个链，该链接受一个问题和检索到的文档并生成一个答案。
from langchain.chains.combine_documents import create_stuff_documents_chain

prompt = ChatPromptTemplate.from_template("""Answer the following question based only on the provided context:
<context>
{context}
</context>
Question: {input}""")

document_chain = create_stuff_documents_chain(llm, prompt)

# 我们希望文档首先来自我们刚刚设置的检索器。 这样，我们可以使用检索器动态选择最相关的文档，并将这些文档传递给给定的问题。
from langchain.chains import create_retrieval_chain

retriever = vector.as_retriever()
retrieval_chain = create_retrieval_chain(retriever, document_chain)

# 我们现在可以调用这个链。这将返回一个字典 - LLM 的响应在键中answer
response = retrieval_chain.invoke({"input": "how can langsmith help with testing?"})
print(response["answer"])
# LangSmith offers several features that can help with testing:...
print('---------------对话检索链-------------------')
'''
对话检索链
到目前为止，我们创建的链只能回答一个问题。人们正在构建的 LLM 应用程序的主要类型之一是聊天机器人。
那么，我们如何把这个链条变成一个可以回答后续问题的链呢？
更新检索
为了更新检索，我们将创建一个新链。此链将接收最新的输入 （） 和对话历史记录 （），并使用 LLM 生成搜索查询。
'''
from langchain.chains import create_history_aware_retriever
from langchain_core.prompts import MessagesPlaceholder

# First we need a prompt that we can pass into an LLM to generate this search query

prompt = ChatPromptTemplate.from_messages([
    MessagesPlaceholder(variable_name="chat_history"),
    ("user", "{input}"),
    ("user", "Given the above conversation, generate a search query to look up to get information relevant to the conversation")
])
retriever_chain = create_history_aware_retriever(llm, retriever, prompt)

# 我们可以通过传递用户提出后续问题的实例来测试这一点。
from langchain_core.messages import HumanMessage, AIMessage

chat_history = [HumanMessage(content="Can LangSmith help test my LLM applications?"), AIMessage(content="Yes!")]
print(retriever_chain.invoke({
    "chat_history": chat_history,
    "input": "Tell me how"
}))
# 这返回了有关在 LangSmith 中进行测试的文档。这是因为 LLM 生成了一个新的查询，将聊天记录与后续问题相结合。
# 现在我们有了这个新的检索器，我们可以创建一个新的链来继续对话，同时记住这些检索到的文档。
prompt = ChatPromptTemplate.from_messages([
    ("system", "Answer the user's questions based on the below context:\n\n{context}"),
    MessagesPlaceholder(variable_name="chat_history"),
    ("user", "{input}"),
])
document_chain = create_stuff_documents_chain(llm, prompt)
retrieval_chain = create_retrieval_chain(retriever_chain, document_chain)

# 我们现在可以对此进行端到端测试：可以看到，这给出了一个连贯的答案——我们已经成功地将我们的检索链变成了一个聊天机器人！
chat_history = [HumanMessage(content="Can LangSmith help test my LLM applications?"), AIMessage(content="Yes!")]
print(retrieval_chain.invoke({
    "chat_history": chat_history,
    "input": "Tell me how"
}))

if __name__ == '__main__':
    print(langchain.__version__)