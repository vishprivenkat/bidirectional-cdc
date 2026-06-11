from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime


@dataclass
class ChangeEvent:
    table_name: str
    operation: str        # INSERT / UPDATE / DELETE
    query_executed: str
    dt_operation_executed: datetime

class BaseMonitor(ABC):

    @abstractmethod
    def start(self):
        pass

    @abstractmethod
    def stop(self):
        pass

    @abstractmethod
    def poll(self) -> list[ChangeEvent]:
        pass