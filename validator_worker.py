import asyncio
import os
from typing import Any, Dict, List

from dotenv import load_dotenv
from langchain.chat_models import init_chat_model
from langchain_core.output_parsers import JsonOutputParser
from langchain_core.prompts import ChatPromptTemplate
from pymongo import AsyncMongoClient

load_dotenv()


class ValidatorWorker:
    def __init__(self):
        self.client = AsyncMongoClient(os.getenv("MONGO_CONNECTION_STRING"))
        self.db = self.client['breaking_bed']
        self.collection = self.db['articles']
        self.model = init_chat_model("command-r7b")
        self.second_class_prompt = self._load_second_class_prompt()
        #self.validator_prompt = self._load_validator_prompt()
        
    def _load_second_class_prompt(self) -> str:
        """Load the validator system prompt from file."""
        with open(
            r'prompts\classifications\base_classification_prompt.txt',
            'r',
            encoding='utf-8'
        ) as file:
            return file.read()
    
    def _load_validator_prompt(self) -> str:
        """Load the validator system prompt from file."""
        with open(
            r'prompts\validator\validator_prompt.txt',
            'r',
            encoding='utf-8'
        ) as file:
            return file.read()    
        
    async def get_articles_to_validate(self) -> List[Dict[str, Any]]:
        """Fetch all articles with state='AGENTS_FINISHED' from MongoDB."""
        cursor = self.collection.find({"state": "AGENTS_FINISHED"})
        articles = await cursor.to_list(length=None)
        return articles
    
    def parse_article_to_message(self, article: Dict[str, Any]) -> Dict[str, Any]:
        """Parse MongoDB article to article_message object."""
        classification_data = article.get('classification', {})
        
        article_message = {
            "article_id": article.get('article_id'),
            "title": article.get('title'),
            "article_body": article.get('article_body'),
            "classification": classification_data.get('classification'),
            "justification": classification_data.get('justification'),
            "confidence": classification_data.get('confidence')
        }
        
        return article_message
    
    async def second_class_article(self, article_message: Dict[str, Any]) -> Dict[str, Any]:
        
        # human_prompt = {k: article_message[k] for k in ("article_id", "classification", "article_body")}
        # Send article to LLM for validation and parse response."""
        # Create the prompt template
        prompt_template = ChatPromptTemplate.from_messages([
            ("system", self.second_class_prompt),
            ("human", "Classify the following article:\n\n{article_body}")
        ])
        
        # Create the chain with JSON output parser
        json_output_parser = JsonOutputParser()
        chain = prompt_template | self.model | json_output_parser
        
        # Invoke the chain
        llm_response = await chain.ainvoke({"article_body": article_message["article_body"]})
        
        # Create the result object
        result = {
            "article_id": article_message['article_id'],
            "agent1": {
                "classification": article_message['classification'],
                "justification": article_message['justification'],
                "confidence": article_message['confidence']
            },
            "agent2": {
                "classification": llm_response.get('classification'),
                "justification": llm_response.get('justification'),
                "confidence": llm_response.get('confidence')
            }
        }
        
        return result
    
    async def validate_article(self, article_message: Dict[str, Any]) -> Dict[str, Any]:
        """Send article to LLM for validation and parse response."""
        # Create the human prompt from article_message
        human_prompt = f"""
        ##### Article to validate #####
        - article_id: {article_message['article_id']}
        - title: {article_message['title']}
        - article_body: {article_message['article_body']}
        
        ##### Agent 1 Classification #####
        - classification: {article_message['classification']}
        - justification: {article_message['justification']}
        - confidence: {article_message['confidence']}
        """
        
        # Create the prompt template
        prompt_template = ChatPromptTemplate.from_messages([
            ("system", self.validator_prompt),
            ("human", human_prompt)
        ])
        
        # Create the chain with JSON output parser
        json_output_parser = JsonOutputParser()
        chain = prompt_template | self.model | json_output_parser
        
        # Invoke the chain
        llm_response = await chain.ainvoke({})
        
        # Create the result object
        result = {
            "article_id": article_message['article_id'],
            "agent1": {
                "classification": article_message['classification'],
                "justification": article_message['justification'],
                "confidence": article_message['confidence']
            },
            "agent2": {
                "classification": llm_response.get('classification'),
                "justification": llm_response.get('justification'),
                "confidence": llm_response.get('confidence')
            }
        }
        
        return result
    
    async def process_articles(self):
        """Main processing loop to validate all articles."""
        print("Starting validator worker...")
        
        # Get articles from MongoDB
        articles = await self.get_articles_to_validate()
        print(f"Found {len(articles)} articles to validate")
        
        # Process each article
        for article in articles:
            try:
                # Parse article to article_message
                article_message = self.parse_article_to_message(article)
                
                print(f"\nProcessing article: {article_message['article_id']}")
                
                # run classification with second model LLM
                result = await self.second_class_article(article_message)
                
                # Validate article with LLM
                #result = await self.validate_article(article_message)

                print(f"Validation result: {result}")
                
            except Exception as e:
                print(f"Error processing article {article.get('article_id')}: {e}")
                continue
    
    async def close(self):
        """Close the MongoDB connection."""
        self.client.close()


async def main():
    worker = ValidatorWorker()
    try:
        await worker.process_articles()
    finally:
        await worker.close()


if __name__ == "__main__":
    asyncio.run(main())
