import asyncio
import json
import os
import uuid
from glob import glob
from typing import Any

from aio_pika import connect_robust, Message, DeliveryMode
from dotenv import load_dotenv
from langchain.chat_models import init_chat_model
from langchain_core.output_parsers import StrOutputParser
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.runnables import RunnableParallel, RunnablePassthrough, RunnableLambda
from pymongo import AsyncMongoClient

from dtos.article import Article
from enums.article_status import ArticleStatus

load_dotenv()

exchange = None

client = AsyncMongoClient("mongodb+srv://noizzhack15_db_user:ELM9TArINOJdqr1f@breakingbadcluster.ndyckke.mongodb.net/?appName=BreakingBadCluster")


async def init_rabbitmq():
    global exchange

    if exchange is None:
        connection = await connect_robust("amqp://guest:guest@localhost/breaking_bed")
        channel = await connection.channel()
        exchange = await channel.declare_exchange(
            "breaking_bed",
            type="topic",
            durable=True)


model = init_chat_model("gpt-4.1-mini")

new_article_message = """
        ##### news article to process #####
        - article_id: {article_id}
        - system_article_id: {system_article_id}
        - title: {title}
        - summary: {summary}
        - article_body: {article_body}
        - author: {author}
        - destination: {destination}
    """


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


async def handle_article_received(data: Any):
    print('received data:')
    print(data)
    await client['breaking_bed']['articles'].insert_one(data)

    return data


async def send_article_to_queue(article_to_send: Article):
    """
    send an article object to a queue
    """
    await init_rabbitmq()
    print("\n--- Tool Execution: send_article_to_queue ---")
    print(f"Article:\n {article_to_send.model_dump()}")
    print("Sending article object to queue successful.")
    print("------------------------------------------\n")

    message = Message(
        body=article_to_send.model_dump_json().encode(),
        delivery_mode=DeliveryMode.PERSISTENT,  # Make the message durable
        content_type='application/json'  # Inform consumers about content type
    )

    await exchange.publish(
        message,
        routing_key="test"
    )

    return "Article successfully sent to the queue."


async def main():
    result = await main_processing_chain.ainvoke(
        {
            "article_id": str(uuid.uuid4()),
            "system_article_id": str(uuid.uuid4()),
            "title": "Exciting Soccer Final in Madrid",
            "summary": "Real Madrid wins the thrilling UEFA Champions League final held in Madrid.",
            "article_body": "In an exhilarating UEFA Champions League final held in Madrid, Real Madrid claimed victory against Liverpool. The match, which took place at Santiago Bernabéu Stadium, saw standout performances from Karim Benzema and Vinícius Júnior, thrilling fans and securing the title for the Spanish giants.",
            "author": "Brittany Johnson",
            "destination": "Norman Morgan",
            "state": ArticleStatus.RECEIVED.name
        })

    print(result)


def init_llm_pipeline_for_topic(desk_prompt: str):
    desk_prompt_template = ChatPromptTemplate.from_messages(
        [
            ("system", desk_prompt),
            ("human", new_article_message)
        ]
    )

    str_output_parser = StrOutputParser()

    return desk_prompt_template | model | str_output_parser


def init_llm_pipeline_for_classification(desk_prompt: str):
    desk_prompt_template = ChatPromptTemplate.from_messages(
        [
            ("system", desk_prompt),
            ("human", new_article_message)
        ]
    )

    str_output_parser = StrOutputParser()

    return desk_prompt_template | model | str_output_parser


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

    handle_article_received_handler = RunnableLambda(handle_article_received)
    handle_article_finished_handler = RunnableLambda(handle_article_finished)

    result = handle_article_received_handler | parallel_processing_chain | handle_article_finished_handler

    return result


main_processing_chain = init_llm_pipeline()

if __name__ == "__main__":
    asyncio.run(main())
