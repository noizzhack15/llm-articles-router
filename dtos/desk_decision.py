from pydantic import BaseModel, Field


class DeskDecision(BaseModel):
    """
    Data Transfer Object for a decision made by the news desk regarding an article.
    """
    desk_name: str = Field(description="the name of the desk.")
    destinations: list[str] = Field(description="the list of destinations to send the article to", default=[])
    reason: str = Field(description="the reason why the desk should handle the article or not.")
    article_domain: str = Field(description="the domain of the article.")
    article_subdomain: str = Field(description="the subdomain of the article.")
    region: str = Field(
        description="the region of the world this article applies to. This can be countries, cities, etc.")
    places: list[str] = Field(
        description="a more specific list of places of the world this article applies to. This can be countries, cities, etc.")
