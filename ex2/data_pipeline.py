import json
import typing
import abc
from typing import Any, List, Sequence, Tuple, Union, Dict


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


class DataStream:

    def __init__(self) -> None:
        self._processors: List[DataProcessor] = []
        self._stats: List[Dict[str, Any]] = []

    def register_processor(self, proc: DataProcessor) -> None:
        self._processors.append(proc)
        self._stats.append({
            "processor": proc,
            "name": proc.__class__.__name__,
            "total_processed": 0
        })

    def process_stream(self, stream: Sequence[Any]) -> None:
        for element in stream:
            routed = False
            for stat in self._stats:
                proc = stat["processor"]
                if proc.validate(element):
                    proc.ingest(element)
                    count = len(element) if isinstance(element, list) else 1
                    stat["total_processed"] += count
                    routed = True
                    break
            if not routed:
                print(
                    "DataStream error - Can't process element in stream: "
                    f"{element}"
                )

    def print_processors_stats(self) -> None:
        print("== DataStream statistics ==")
        if not self._processors:
            print("No processor found, no data")
            return

        for stat in self._stats:
            proc = stat["processor"]
            remaining = len(proc._internal_data)
            display_name = stat["name"].replace("Processor", "Processor")
            print(
                f"{display_name}: total {stat['total_processed']} "
                f"items processed, remaining {remaining} on processor"
            )


@typing.runtime_checkable
class ExportPlugin(typing.Protocol):

    def process_output(self, data: List[Tuple[int, str]]) -> None:
        ...


class CSVExportPlugin:

    def process_output(self, data: List[Tuple[int, str]]) -> None:
        if not data:
            return
        raw_values = [item[1] for item in data]
        print(f"CSV Output:\n{','.join(raw_values)}")


class JSONExportPlugin:

    def process_output(self, data: List[Tuple[int, str]]) -> None:
        if not data:
            return
        out_dict = {f"item_{item[0]}": item[1] for item in data}
        print(f"JSON Output:\n{json.dumps(out_dict)}")


class PipelineDataStream(DataStream):

    def output_pipeline(self, nb: int, plugin: ExportPlugin) -> None:
        for stat in self._stats:
            proc: DataProcessor = stat["processor"]
            collected_data: List[Tuple[int, str]] = []

            for _ in range(nb):
                if len(proc._internal_data) > 0:
                    collected_data.append(proc.output())
                else:
                    break

            if collected_data:
                plugin.process_output(collected_data)


if __name__ == "__main__":
    print("=== Code Nexus - Data Pipeline ===")
    print("Initialize Data Stream...")

    pipeline = PipelineDataStream()
    pipeline.print_processors_stats()

    print("Registering Processors")
    num_p = NumericProcessor()
    text_p = TextProcessor()
    log_p = LogProcessor()

    pipeline.register_processor(num_p)
    pipeline.register_processor(text_p)
    pipeline.register_processor(log_p)

    log_batch_1 = [
        {
            'log_level': 'WARNING',
            'log_message': 'Telnet access!\nUse ssh instead'
        },
        {
            'log_level': 'INFO',
            'log_message': 'User wil is connected'
        }
    ]

    batch1: Sequence[Any] = [
        'Hello world',
        [3.14, -1, 2.71],
        log_batch_1,
        42,
        ['Hi', 'five']
    ]

    print("Send first batch of data on stream:")
    pipeline.process_stream(batch1)
    pipeline.print_processors_stats()

    print("Send 3 processed data from each processor to a CSV plugin:")
    csv_plugin = CSVExportPlugin()
    pipeline.output_pipeline(3, csv_plugin)
    pipeline.print_processors_stats()

    log_batch_2 = [
        {
            'log_level': 'ERROR',
            'log_message': '500 server crash'
        },
        {
            'log_level': 'NOTICE',
            'log_message': 'Certificate expires in 10 days'
        }
    ]

    batch2: Sequence[Any] = [
        21,
        ['I love AI', 'LLMs are wonderful', 'Stay healthy'],
        log_batch_2,
        [32, 42, 64, 84, 128, 168],
        'World hello'
    ]

    print("Send another batch of data:")
    pipeline.process_stream(batch2)
    pipeline.print_processors_stats()

    print("Send 5 processed data from each processor to a JSON plugin:")
    json_plugin = JSONExportPlugin()
    pipeline.output_pipeline(5, json_plugin)
    pipeline.print_processors_stats()
