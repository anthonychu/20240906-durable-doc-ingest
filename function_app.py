import json
import logging
import os
from typing import List

import azure.durable_functions as df
import azure.functions as func
import requests
from azure.storage.blob.aio import BlobServiceClient
from langchain_community.document_loaders.pdf import PyMuPDFLoader
from langchain_core.documents.base import Document
from langchain_text_splitters import CharacterTextSplitter

from common import vector_store

myApp = df.DFApp(http_auth_level=func.AuthLevel.ANONYMOUS)

blob_service_client = BlobServiceClient.from_connection_string(os.environ["AzureWebJobsStorage"])

logger = logging.getLogger('azure')
logger.setLevel(logging.ERROR)


# This function starts the orchestrator
@myApp.route(route="start_orchestrator")
@myApp.durable_client_input(client_name="client")
async def http_start(req: func.HttpRequest, client: df.DurableOrchestrationClient):
    instance_id = await client.start_new("pdf_ingest_orchestrator", client_input=req.params.get("pdf_url"))
    response = client.create_check_status_response(req, instance_id)
    return response


# Orchestrator
@myApp.orchestration_trigger(context_name="context")
def pdf_ingest_orchestrator(context: df.DurableOrchestrationContext):
    pdf_url = context.get_input()
    download_and_split_pdf_result: dict = yield context.call_activity("download_and_split_pdf", {"pdf_url":pdf_url, "instance_id":context.instance_id})

    num_docs = download_and_split_pdf_result["num_docs"]
    doc_url = download_and_split_pdf_result["doc_url"]

    yield context.call_activity("delete_docs_for_pdf", {"pdf_url":doc_url})

    tasks = [context.call_activity("ingest_doc", {"i":i, "instance_id":context.instance_id}) for i in range(num_docs)]

    yield context.task_all(tasks)

    return "Done"


@myApp.activity_trigger(input_name="input")
async def download_and_split_pdf(input: dict):
    doc_url = input["pdf_url"]
    instance_id = input["instance_id"]

    folder = instance_id

    r = requests.get(doc_url)
    filename = doc_url.split("/")[-1]
    filepath = f"{folder}/{filename}"

    print(f"Downloading {doc_url} to {filepath}")

    os.makedirs(folder, exist_ok=True)

    with open(filepath, "wb") as f:
        f.write(r.content)

    container_client = blob_service_client.get_container_client("output")

    try:
        text_splitter = CharacterTextSplitter(chunk_size=1000, chunk_overlap=50)
        loader = PyMuPDFLoader(file_path=filepath)
        docs: List[Document] = loader.load_and_split(text_splitter=text_splitter)
        print(f"Split into {len(docs)} documents")

        # fix sources
        for doc in docs:
            doc.metadata["source"] = doc_url
            doc.metadata["file_path"] = doc_url

        try:
            await blob_service_client.create_container("output")
        except Exception as e:
            print("container already exists")

        for i, doc in enumerate(docs):
            blob_path = f"{folder}/{i}.json"
            print(f"Uploading {blob_path}")
            await container_client.upload_blob(name=blob_path, data=doc.json().encode("utf-8"))

    finally:
        os.remove(filepath)
        os.rmdir(folder)
        await container_client.close()
    
    return {
        "num_docs": len(docs),
        "doc_url": doc_url,
    }


@myApp.activity_trigger(input_name="input")
def delete_docs_for_pdf(input: dict):
    doc_url = input["pdf_url"]
    print(f"Deleting existing documents for file {doc_url}")
    existing_docs = vector_store.client.search(filter=f"source eq '{doc_url}'", select="id")
    existing_doc_ids = [doc["id"] for doc in existing_docs]
    vector_store.delete(existing_doc_ids)


@myApp.activity_trigger(input_name="input")
async def ingest_doc(input: dict):
    i = input["i"]
    instance_id = input["instance_id"]

    container_client = blob_service_client.get_container_client("output")

    blob_path = f"{instance_id}/{i}.json"
    blob_client = container_client.get_blob_client(blob_path)
    blob = await blob_client.download_blob()
    doc_dict = json.loads(await blob.readall())
    doc = Document(page_content=doc_dict["page_content"], metadata=doc_dict["metadata"])
    print(f"ingesting {doc.metadata['page']}")

    await vector_store.aadd_documents([doc])

    await blob_client.close()
    await container_client.close()
