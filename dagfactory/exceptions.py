"""Module contains exceptions for dag-factory"""
from typing import List, Optional

class DagFactoryException(Exception):
    """
    Base class for all dag-factory errors.
    """


class DagFactoryConfigException(Exception):
    """
    Raise for dag-factory config errors.
    """
    def __init__(self, msg: str, dag_id: Optional[str]="", tags: Optional[List[str]]=[]):
        self.tags = tags
        self.dag_id = dag_id
        super().__init__(msg)