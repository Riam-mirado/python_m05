import abc
from typing import Any, List, Dict, Union, Sequence


class DataProcessor(abc.ABC):
    def __init__(self) -> None:
        self._internal_data: List[str] = []
        self._counter: int = 0

    @abc.abstractmethod
    def validate(self, data: Any) -> bool:
        pass

    @abc.abstractmethod
    def ingest(self, data: Any) -> None:
        pass

    def output(self) -> tuple[int, str]:
        if not self._internal_data:
            raise IndexError("No data available to output.")

        data = self._internal_data.pop(0)
        rank = self._counter
        self._counter += 1
        return (rank, data)


class NumericProcessor(DataProcessor):
    """Processeur dédié aux entiers, flottants et listes de ces types."""

    def validate(self, data: Any) -> bool:
        if isinstance(data, (int, float)):
            return True
        if isinstance(data, list):
            return (all(isinstance(x, (int, float))
                        for x in data) and len(data) > 0
                    )
        return False

    def ingest(self, data: Union[int, float, Sequence[Union[int, float]]]
               ) -> None:
        if not self.validate(data):
            raise ValueError("Improper numeric data")

        if isinstance(data, list):
            for x in data:
                self._internal_data.append(str(x))
        else:
            self._internal_data.append(str(data))


class TextProcessor(DataProcessor):

    def validate(self, data: Any) -> bool:
        if isinstance(data, str):
            return True
        if isinstance(data, list):
            return all(isinstance(x, str) for x in data) and len(data) > 0
        return False

    def ingest(self, data: Union[str, List[str]]) -> None:
        if not self.validate(data):
            raise ValueError("Improper text data")

        if isinstance(data, list):
            for x in data:
                self._internal_data.append(x)
        else:
            self._internal_data.append(data)


class LogProcessor(DataProcessor):

    def validate(self, data: Any) -> bool:
        def is_valid_log(d: Any) -> bool:
            return (isinstance(d, dict) and
                    "log_level" in d and "log_message" in d and
                    isinstance(d["log_level"], str) and
                    isinstance(d["log_message"], str))

        if is_valid_log(data):
            return True
        if isinstance(data, list):
            return all(is_valid_log(x) for x in data) and len(data) > 0
        return False

    def ingest(self, data: Union[Dict[str, str], List[Dict[str, str]]]
               ) -> None:
        if not self.validate(data):
            raise ValueError("Improper log data")

        def format_log(d: Dict[str, str]) -> str:
            return f"{d['log_level']}: {d['log_message']}"

        if isinstance(data, list):
            for x in data:
                self._internal_data.append(format_log(x))
        else:
            self._internal_data.append(format_log(data))


if __name__ == "__main__":
    print("=== Code Nexus - Data Processor ===")
    print()
    num_proc = NumericProcessor()
    text_proc = TextProcessor()
    log_proc = LogProcessor()

    print("Testing Numeric Processor...")
    print(f" Trying to validate input '42': {num_proc.validate(42)}")
    print(f" Trying to validate input 'Hello': {num_proc.validate('Hello')}")

    print(" Test invalid ingestion of string 'foo' without prior validation:")
    try:
        num_proc.ingest("foo")  # type: ignore
    except ValueError as e:
        print(f" Got exception: {e}")

    numeric_test_data = [1, 2, 3, 4, 5]
    print(f" Processing data: {numeric_test_data}")
    num_proc.ingest(numeric_test_data)

    print(" Extracting 3 values...")
    for _ in range(3):
        rank, val = num_proc.output()
        print(f" Numeric value {rank}: {val}")
    print()

    print("Testing Text Processor...")
    print(f" Trying to validate input '42': {text_proc.validate(42)}")

    text_test_data = ['Hello', 'Nexus', 'World']
    print(f" Processing data: {text_test_data}")
    text_proc.ingest(text_test_data)

    print(" Extracting 1 value...")
    rank, val = text_proc.output()
    print(f" Text value {rank}: {val}")
    print()
    print("Testing Log Processor...")
    print(f" Trying to validate input 'Hello': {log_proc.validate('Hello')}")

    log_test_data = [
        {'log_level': 'NOTICE', 'log_message': 'Connection to server'},
        {'log_level': 'ERROR', 'log_message': 'Unauthorized access!!'}
    ]
    print(f" Processing data: {log_test_data}")
    log_proc.ingest(log_test_data)

    print(" Extracting 2 values...")
    for _ in range(2):
        rank, val = log_proc.output()
        print(f" Log entry {rank}: {val}")
