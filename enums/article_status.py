from enum import Enum


class ArticleStatus(Enum):
    RECEIVED = "received"
    STARTED = "started"
    AGENTS_FINISHED = "agents_finished"
