from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field


class Article(BaseModel):
    """
    Data Transfer Object for a generated article.
    """
    article_id: Optional[str] = Field(description="The article ID", default=None)
    title: str = Field(description="the professional title of the article.")
    article_body: str = Field(description="the complete, polished body of the article.")
    source: Optional[str] = Field(description="the name of the person who wrote the article.", default=None)
    publisher: Optional[str] = Field(description="the source of the article.", default=None)
    publication_date: Optional[datetime] = Field(description="the publication date of the article.", default=None)
    recipients: Optional[list[str]] = Field(
        description="a list of people that should receive the article.",
        default=None)
