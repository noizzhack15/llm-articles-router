from enum import Enum


class ArticleStatus(Enum):
    RECEIVED = "received"
    STARTED = "started"
    FINISHED_CLASSIFICATION = "finished_classification"
    FINISHED_FINALIZATION = "finished_finalization"
    AGENTS_FINISHED = "agents_finished"
