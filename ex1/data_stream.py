from typing import Any, Dict, List, Sequence
from ex0.data_processor import (
    DataProcessor,
    LogProcessor,
    NumericProcessor,
    TextProcessor,
)


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


if __name__ == "__main__":
    print("=== Code Nexus - Data Stream ===")
    print("Initialize Data Stream...")

    stream_manager = DataStream()
    stream_manager.print_processors_stats()

    print("Registering Numeric Processor")
    num_processor = NumericProcessor()
    stream_manager.register_processor(num_processor)

    log_batch = [
        {
            'log_level': 'WARNING',
            'log_message': 'Telnet access!\nUse ssh instead'
        },
        {
            'log_level': 'INFO',
            'log_message': 'User wil is connected'
        }
    ]

    first_batch: Sequence[Any] = [
        'Hello world',
        [3.14, -1, 2.71],
        log_batch,
        42,
        ['Hi', 'five']
    ]

    print("Send first batch of data on stream:")
    stream_manager.process_stream(first_batch)
    stream_manager.print_processors_stats()

    print("Registering other data processors")
    text_processor = TextProcessor()
    log_processor = LogProcessor()
    stream_manager.register_processor(text_processor)
    stream_manager.register_processor(log_processor)

    print("Send the same batch again")
    stream_manager.process_stream(first_batch)
    stream_manager.print_processors_stats()

    print(
        "Consume some elements from the data processors: "
        "Numeric 3, Text 2, Log 1"
    )
    for _ in range(3):
        num_processor.output()
    for _ in range(2):
        text_processor.output()
    for _ in range(1):
        log_processor.output()

    stream_manager.print_processors_stats()
