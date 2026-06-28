import json
import typing
from typing import Any, List, Sequence, Tuple
from ex0.data_processor import (
    DataProcessor,
    LogProcessor,
    NumericProcessor,
    TextProcessor,
)
from ex1.data_stream import DataStream


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
