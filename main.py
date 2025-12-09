import asyncio
import json
import logging
import os
import uuid
from glob import glob
from typing import Any

from aio_pika import connect_robust
from aio_pika.abc import AbstractIncomingMessage
from dotenv import load_dotenv
from langchain.chat_models import init_chat_model
from langchain_core.output_parsers import StrOutputParser, JsonOutputParser
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
        queue = await channel.declare_queue(
            "test",
            durable=True
        )

        await queue.consume(on_message)
        await asyncio.Future()


async def on_message(message: AbstractIncomingMessage):
    try:
        # async with message.process():  # Acknowledge the message upon successful processing
        message_str = message.body.decode()

        message_data = json.loads(message_str)

        data = {
            **message_data,
            "system_article_id": str(uuid.uuid4()),
            "state": ArticleStatus.RECEIVED.name
        }

        print(f"Received message: {message_str}")

        await client['breaking_bed']['articles1'].insert_one(data)

        result = await main_processing_chain.ainvoke(data)

        print(f"result: {result}")
    except Exception as ex:
        logging.getLogger().error(ex)


model = init_chat_model("gpt-4.1-mini")

new_article_message = """
        ##### news article to process #####
        - article_id: {article_id}
        - system_article_id: {system_article_id}
        - title: {title}
        - article_body: {article_body}
    """

new_base_classification_message = """
        ##### news classification to process #####
        - classification: {classification}
        - justification: {justification}
        - confidence: {confidence}
    """


async def debug(data: dict):
    return data


async def handle_article_finished(data: dict):
    print(data)

    agents_results = []
    for key, agents_result in data.items():
        if key == 'article':
            continue

        agents_results.append(agents_result)

    await client['breaking_bed']['articles1'].find_one_and_update(
        {
            "article_id": data['article']['article_id'],
            "system_article_id": data['article']['system_article_id']
        },
        {
            "$set": {
                "state": ArticleStatus.FINISHED.name,
                "agents_results": agents_results
            }
        })

    return data


async def handle_classification_processing_started(data: Any):
    return {
        **data['base_classification'],
        **data['article']
    }


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

    return data


def init_llm_pipeline_for_topic(desk_prompt: str):
    desk_prompt_template = ChatPromptTemplate.from_messages(
        [
            ("system", desk_prompt),
            ("human", new_article_message)
        ]
    )

    str_output_parser = StrOutputParser()

    return desk_prompt_template | model | str_output_parser


def init_llm_pipeline_for_base_classification():
    with open(
            r'C:\code_projects\llm-articles-router\prompts\classifications\base_classification_prompt.txt',
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
            r'C:\code_projects\llm-articles-router\prompts\classifications\classification_prompt.txt',
            'r',
            encoding='utf-8') as file:
        base_classification_prompt = file.read()

    base_classification_prompt_template = ChatPromptTemplate.from_messages(
        [
            ("system", base_classification_prompt),
            ("human", new_base_classification_message)
        ]
    )

    classification_processing_started_handler = RunnableLambda(handle_classification_processing_started)
    de = RunnableLambda(debug)

    str_output_parser = StrOutputParser()

    parallel_processing_chain = RunnableParallel(
        classification=classification_processing_started_handler | base_classification_prompt_template | model | str_output_parser,
        article=RunnablePassthrough()
    )

    return de | parallel_processing_chain | de


def init_llm_pipeline():
    prompt_files = glob(r"C:\code_projects\llm-articles-router\prompts\eng\*.txt")

    desks_llm_pipelines: dict[str, Any] = {
        "article": RunnablePassthrough()
    }

    # for prompt_file in prompt_files:
    #     desk_topic = os.path.basename(prompt_file).split('_')[0]
    #
    #     with open(
    #             prompt_file,
    #             'r',
    #             encoding='utf-8') as file:
    #         desk_prompt = file.read()
    #
    #         llm_pipeline_for_topic = init_llm_pipeline_for_topic(desk_prompt)
    #         desks_llm_pipelines[f'{desk_topic}_desk'] = llm_pipeline_for_topic

    llm_pipeline_for_base_classification = init_llm_pipeline_for_base_classification()
    llm_pipeline_for_classification = init_llm_pipeline_for_classification()
    desks_llm_pipelines['base_classification'] = llm_pipeline_for_base_classification
    parallel_processing_chain = RunnableParallel(
        **desks_llm_pipelines
    )

    handle_article_received_handler = RunnableLambda(handle_article_processing_started)
    handle_article_finished_handler = RunnableLambda(handle_article_finished)

    result = (handle_article_received_handler |
              parallel_processing_chain |
              llm_pipeline_for_classification |
              handle_article_finished_handler
              )

    return result


main_processing_chain = init_llm_pipeline()

if __name__ == "__main__":
    asyncio.run(start_rabbitmq(), debug=True)
