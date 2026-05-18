from abc import ABC, abstractmethod
from typing import Any


class DataProcessor(ABC):
    def __init__(self):
        self._data_store = []
        self._total_count = 0

    @abstractmethod
    def validate(self, data: Any) -> bool:
        pass

    @abstractmethod
    def ingest(self, data: Any) -> None:
        pass

    def output(self) -> tuple[int, str]:
        pass
        return (0, "")


class NumericProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        pass
        return True

    def ingest(self, data: Any) -> None:
        pass


class TextProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        pass
        return True

    def ingest(self, data: Any) -> None:
        pass


class LogProcessor:
    def validate(self, data: Any) -> bool:
        pass
        return True

    def ingest(self, data: Any) -> None:
        pass
