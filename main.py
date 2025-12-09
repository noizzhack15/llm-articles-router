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
from langchain_core.runnables import RunnableParallel, RunnablePassthrough, RunnableLambda, RunnableBranch
from pymongo import AsyncMongoClient

from enums.article_status import ArticleStatus

load_dotenv()

queue = None

client = AsyncMongoClient(os.getenv("MONGO_CONNECTION_STRING"))
str_output_parser = StrOutputParser()
json_output_parser = JsonOutputParser()


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

        await client['breaking_bed']['articles'].insert_one(data)

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


async def debugger(data: dict):
    return data


debugger_handler = RunnableLambda(debugger)


async def handle_guardrails_finished(data: dict):
    return data


async def handle_guardrails_finished_successfully(data: dict):
    del data["article"]["_id"]
    return data['article']


async def handle_article_finished(data: dict):
    print(data)
    agents_results = []

    for key, agents_result in data.items():
        if key == 'article':
            continue

        agents_results.append(json.loads(agents_result))

    await client['breaking_bed']['articles'].find_one_and_update(
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


def init_llm_guardrails_pipline():
    with open(
            r"C:\code_projects\llm-articles-router\prompts\guardrails\privacy_inspection_prompt.txt",
            'r',
            encoding='utf-8') as file:
        guardrails_prompt = file.read()

    guardrails_prompt_template = ChatPromptTemplate.from_messages(
        [
            ("system", guardrails_prompt),
            ("human", new_article_message)
        ]
    )

    handle_guardrails_finished_handler = RunnableLambda(handle_guardrails_finished)

    guardrails_llm_pipelines: dict[str, Any] = {
        "article": RunnablePassthrough(),
        "guardrails_result": guardrails_prompt_template | model | json_output_parser
    }

    return RunnableParallel(
        guardrails_llm_pipelines
    ) | handle_guardrails_finished_handler


def check_violates_privacy_regulations(data: dict):
    return not data["guardrails_result"]["violates_privacy_regulations"]


def init_llm_pipeline():
    prompt_files = glob(r"C:\code_projects\llm-articles-router\prompts\eng\*.txt")

    desks_llm_pipelines: dict[str, Any] = {
        "article": RunnablePassthrough()
    }

    for prompt_file in prompt_files:
        desk_topic = os.path.basename(prompt_file).split('_')[0]

        with open(
                prompt_file,
                'r',
                encoding='utf-8') as file:
            desk_prompt = file.read()

            llm_pipeline_for_topic = init_llm_pipeline_for_topic(desk_prompt)
            desks_llm_pipelines[f'{desk_topic}_desk'] = llm_pipeline_for_topic

    parallel_processing_chain = RunnableParallel(
        **desks_llm_pipelines
    )

    handle_article_received_handler = RunnableLambda(handle_article_processing_started)
    handle_article_finished_handler = RunnableLambda(handle_article_finished)
    handle_guardrails_finished_successfully_handler = RunnableLambda(handle_guardrails_finished_successfully)

    guardrails_pipeline = init_llm_guardrails_pipline()
    message_passed_guardrails_pipeline = handle_guardrails_finished_successfully_handler | parallel_processing_chain | handle_article_finished_handler

    branch = RunnableBranch(
        (check_violates_privacy_regulations, message_passed_guardrails_pipeline),
        handle_article_finished_handler
    )

    result = (handle_article_received_handler |
              guardrails_pipeline |
              branch)

    return result


def init_llm_pipeline_for_topic(desk_prompt: str):
    desk_prompt_template = ChatPromptTemplate.from_messages(
        [
            ("system", desk_prompt),
            ("human", new_article_message)
        ]
    )

    return debugger_handler | desk_prompt_template | model | str_output_parser


main_processing_chain = init_llm_pipeline()

if __name__ == "__main__":
    asyncio.run(start_rabbitmq(), debug=True)
