import asyncio
import json
import logging
import os
import uuid
from typing import Any

from aio_pika import connect_robust
from aio_pika.abc import AbstractIncomingMessage
from dotenv import load_dotenv
from langchain.chat_models import init_chat_model
from langchain_core.output_parsers import JsonOutputParser, StrOutputParser
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.runnables import RunnableParallel, RunnablePassthrough, RunnableLambda
from pymongo import AsyncMongoClient

from enums.article_status import ArticleStatus

load_dotenv()

queue = None

client = AsyncMongoClient(os.getenv("MONGO_CONNECTION_STRING"))


async def start_rabbitmq():
    global queue

    if queue is None:
        connection = await connect_robust(os.getenv("RABBITMQ_CONNECTION_STRING"))
        channel = await connection.channel()
        await channel.set_qos(10)
        queue = await channel.declare_queue(
            "breaking_feed_queues",
            durable=True,

        )

        await queue.consume(on_message)
        await asyncio.Future()


async def on_message(message: AbstractIncomingMessage):
    try:
        async with message.process():  # Acknowledge the message upon successful processing
            message_str = message.body.decode()

            message_data = json.loads(message_str)

            data = {
                **message_data,
                "system_article_id": str(uuid.uuid4()),
                "state": ArticleStatus.RECEIVED.name
            }

            print(f"Received message: {message_str}")

            await client['breaking_bed']['articles'].insert_one(data)

            await asyncio.sleep(1)

            result = await main_processing_chain.ainvoke(data)

            print(f"result: {result}")
    except Exception as ex:
        logging.getLogger().error(ex)


model = init_chat_model("gpt-4.1-mini")

# model = ChatCohere(
#     model="command-a-03-2025"
# )

new_article_message = """
        ##### news article to process #####
        - article_id: {article_id}
        - system_article_id: {system_article_id}
        - title: {title}
        - article_body: {article_body}
    """

classification_message = """
        ##### news classification to process #####
        - classification: {classification}
        - justification: {justification}
        - confidence: {confidence}
    """

finalization_message = """
        ##### news finalization  to process #####
        - base_classification: {base_classification}
        - classification: {classification}
        - article: {article}
    """


async def handle_article_finished(data: dict):
    print(data)

    await asyncio.sleep(1)

    await client['breaking_bed']['articles'].find_one_and_update(
        {
            "article_id": data['article']['article_id'],
            "system_article_id": data['article']['system_article_id']
        },
        {
            "$set": {
                "state": ArticleStatus.AGENTS_FINISHED.name,
                "classification": data["classification"],
                "base_classification": data["base_classification"],
                "finalization": data["finalization"]
            }
        })

    return data


async def handle_classification_finished(data: dict):
    print(data)

    await asyncio.sleep(1)

    await client['breaking_bed']['articles'].find_one_and_update(
        {
            "article_id": data['article']['article_id'],
            "system_article_id": data['article']['system_article_id']
        },
        {
            "$set": {
                "state": ArticleStatus.FINISHED_CLASSIFICATION.name
            }
        })

    return data


async def handle_finalization_finished(data: dict):
    print(data)

    await asyncio.sleep(1)

    await client['breaking_bed']['articles'].find_one_and_update(
        {
            "article_id": data['article']['article_id'],
            "system_article_id": data['article']['system_article_id']
        },
        {
            "$set": {
                "state": ArticleStatus.FINISHED_FINALIZATION.name
            }
        })

    return data


async def handle_classification_processing_started(data: Any):
    return {
        **data['base_classification'],
        **data['article']
    }


async def get_article(data: Any):
    return data['article']


async def get_base_classification(data: Any):
    return data['base_classification']


async def get_classification(data: Any):
    return data['classification']


async def debug(data: Any):
    return data


get_article_data = RunnableLambda(get_article)
get_base_classification_data = RunnableLambda(get_base_classification)
get_classification_data = RunnableLambda(get_classification)
d = RunnableLambda(debug)


async def handle_article_processing_started(data: Any):
    print('received data:')
    print(data)

    await client['breaking_bed']['articles'].find_one_and_update(
        {
            "article_id": data['article_id'],
            "system_article_id": data['system_article_id']
        },
        {
            "$set": {
                "state": ArticleStatus.STARTED.name
            }
        })

    await asyncio.sleep(1)

    return data


def init_llm_pipeline_for_base_classification():
    with open(
            r'.\prompts\classifications\base_classification_prompt.txt',
            'r',
            encoding='utf-8') as file:
        base_classification_prompt = file.read()

    base_classification_prompt_template = ChatPromptTemplate.from_messages(
        [
            ("system", base_classification_prompt),
            ("human", new_article_message)
        ]
    )

    json_output_parser = JsonOutputParser()

    return base_classification_prompt_template | model | json_output_parser


def init_llm_pipeline_for_classification():
    with open(
            r'.\prompts\classifications\classification_prompt.txt',
            'r',
            encoding='utf-8') as file:
        classification_prompt = file.read()

    classification_prompt_template = ChatPromptTemplate.from_messages(
        [
            ("system", classification_prompt),
            ("human", classification_message)
        ]
    )

    classification_processing_started_handler = RunnableLambda(handle_classification_processing_started)

    json_output_parser = JsonOutputParser()

    parallel_processing_chain = RunnableParallel(
        classification=classification_processing_started_handler | classification_prompt_template | model | json_output_parser,
        article=RunnablePassthrough() | get_article_data,
        base_classification=RunnablePassthrough() | get_base_classification_data,
    )

    return parallel_processing_chain


def init_llm_pipeline_for_finalization():
    with open(
            r'.\prompts\classifications\finalizer.txt',
            'r',
            encoding='utf-8') as file:
        finalization_prompt = file.read()

    finalization_prompt_template = ChatPromptTemplate.from_messages(
        [
            ("system", finalization_prompt),
            ("human", finalization_message)
        ]
    )

    str_output_parser = StrOutputParser()

    finished = RunnableLambda(handle_finalization_finished)

    parallel_processing_chain = RunnableParallel(
        finalization=finalization_prompt_template | model | str_output_parser,
        classification=RunnablePassthrough() | get_classification_data,
        article=RunnablePassthrough() | get_article_data,
        base_classification=RunnablePassthrough() | get_base_classification_data,
    )

    return parallel_processing_chain | finished


def init_llm_pipeline():
    desks_llm_pipelines: dict[str, Any] = {
        "article": RunnablePassthrough()
    }

    finished = RunnableLambda(handle_classification_finished)

    llm_pipeline_for_base_classification = init_llm_pipeline_for_base_classification()
    llm_pipeline_for_classification = init_llm_pipeline_for_classification()
    llm_pipeline_for_finalization = init_llm_pipeline_for_finalization()
    desks_llm_pipelines['base_classification'] = llm_pipeline_for_base_classification
    parallel_processing_chain = RunnableParallel(
        **desks_llm_pipelines
    ) | finished

    handle_article_received_handler = RunnableLambda(handle_article_processing_started)
    handle_article_finished_handler = RunnableLambda(handle_article_finished)

    result = (handle_article_received_handler |
              parallel_processing_chain |
              llm_pipeline_for_classification |
              llm_pipeline_for_finalization |
              handle_article_finished_handler
              )

    return result


main_processing_chain = init_llm_pipeline()

if __name__ == "__main__":
    asyncio.run(start_rabbitmq(), debug=True)
