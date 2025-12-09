from enum import Enum


class ArticleStatus(Enum):
    RECEIVED = "received"
    STARTED = "started"
    FINISHED = "finished"
    NOT_PASSED_PRIVACY_CHECKS = "not_passed_privacy_checks"
