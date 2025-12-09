from pydantic import BaseModel, Field


class ArticlePrivacyRegulationsViolationInspectionResult(BaseModel):
    """
    Data model for article privacy regulation violation inspection result.
    """
    violates_privacy_regulations: bool = Field(description="if the article violates privacy regulations or not")
    reason: str = Field(description="the reason why the article violates privacy regulations or not.")
