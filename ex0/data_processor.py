from abc import ABC, abstractmethod
from typing import Any, List, Tuple, Dict, Union

pt = print


class DataProcessor(ABC):
    def __init__(self) -> None:
        self._data_store: List[str] = []
        self._total_count: int = 0

    @abstractmethod
    def validate(self, data: Any) -> bool:
        pass

    @abstractmethod
    def ingest(self, data: Any) -> None:
        pass

    def output(self) -> Tuple[int, str]:
        if not self._data_store:
            raise IndexError("No data available in processor")

        rank = self._total_count - len(self._data_store)
        value = self._data_store.pop(0)
        return rank, value


class NumericProcessor(DataProcessor):

    def validate(self, data: Any) -> bool:
        if isinstance(data, (int, float)):
            return True
        if isinstance(data, list):
            return all(isinstance(x, (int, float)) for x in data)
        return False

    def ingest(self, data: Union[int, float, List[Union[int, float]]]) -> None:
        if not self.validate(data):
            raise ValueError("Improper numeric data")

        items = data if isinstance(data, list) else [data]
        for item in items:
            self._data_store.append(str(item))
            self._total_count += 1


class TextProcessor(DataProcessor):

    def validate(self, data: Any) -> bool:
        if isinstance(data, str):
            return True
        if isinstance(data, list):
            return all(isinstance(x, str) for x in data)
        return False

    def ingest(self, data: Union[str, List[str]]) -> None:
        if not self.validate(data):
            raise ValueError("Improper text data")

        items = data if isinstance(data, list) else [data]
        for item in items:
            self._data_store.append(item)
            self._total_count += 1


class LogProcessor(DataProcessor):

    def validate(self, data: Any) -> bool:
        def is_valid_log(d: Any) -> bool:
            return (isinstance(d, dict) and
                    all(isinstance(k, str) and isinstance(v, str)
                        for k, v in d.items()))

        if is_valid_log(data):
            return True
        if isinstance(data, list):
            return all(is_valid_log(x) for x in data)
        return False

    def ingest(
        self, data: Union[Dict[str, str], List[Dict[str, str]]]
    ) -> None:
        if not self.validate(data):
            raise ValueError("Improper log data")

        entries = data if isinstance(data, list) else [data]
        for entry in entries:
            formatted_log = ", ".join([f"{k}: {v}" for k, v in entry.items()])
            self._data_store.append(formatted_log)
            self._total_count += 1


def main() -> None:
    pt("=== Code Nexus Data Processor ===")

    pt("\nTesting Numeric Processor...")
    num_proc = NumericProcessor()
    pt(f"Trying to validate input '42': {num_proc.validate(42)}")
    pt(f"Trying to validate input 'Hello': {num_proc.validate('Hello')}")

    try:
        pt("Test invalid ingestion of string 'foo' without prior validation:")
        num_proc.ingest("foo")  # type: ignore
    except ValueError as e:
        pt(f"Got exception: {e}")

    num_proc.ingest([1, 2, 3, 4, 5])
    pt("Extracting 3 values...")
    for i in range(3):
        rank, val = num_proc.output()
        pt(f"Numeric value {rank}: {val}")

    pt("\nTesting Text Processor...")
    text_proc = TextProcessor()
    text_proc.ingest(["Hello", "Nexus", "World"])
    rank, val = text_proc.output()
    pt(f"Text value {rank}: {val}")

    pt("\nTesting Log Processor...")
    log_proc = LogProcessor()
    logs = [
        {"log_level": "NOTICE", "log_message": "Connection to server"},
        {"log_level": "ERROR", "log_message": "Unauthorized access!!!"}
    ]
    log_proc.ingest(logs)
    for i in range(2):
        rank, val = log_proc.output()
        pt(f"Log entry {rank}: {val}")


if __name__ == "__main__":
    main()
