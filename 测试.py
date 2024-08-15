import uvicorn
from langchain import hub
prompt = hub.pull("hwchase17/openai-functions-agent")
import numpy as np
from fastapi import FastAPI
from typing import List

from langchain.agents import AgentExecutor
from langchain.pydantic_v1 import BaseModel, Field
from langchain_core.messages import BaseMessage
from langserve import add_routes
from langchain_community.document_loaders import UnstructuredWordDocumentLoader,UnstructuredFileLoader
loader = UnstructuredWordDocumentLoader("./paper1.docx")
doc_data = loader.load()
app = FastAPI(
  title="LangChain Server",
  version="1.0",
  description="A simple API server using LangChain's Runnable interfaces",
)
from docx import Document

def load_docx(file_path):
    doc = Document(file_path)
    full_text = []
    for para in doc.paragraphs:
        full_text.append(para.text)
    return "\n".join(full_text)

doc_text = load_docx("your_document.docx")
print(doc_text)
