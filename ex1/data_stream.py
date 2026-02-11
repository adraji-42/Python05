from abc import ABC, abstractmethod
from typing import Any, List, Dict, Union, Optional


class DataStream(ABC):

    def __init__(self, stream_id: str, stream_type: str) -> None:

        self.stream_id: str = stream_id
        self.stream_type: str = stream_type
        self.total_processed: int = 0
        self.error_count: int = 0

    @abstractmethod
    def process_batch(self, data_batch: List[Any]) -> str:

        pass

    def filter_data(
        self, data_batch: List[Any],
        criteria: Optional[str] = "normal-priority"
    ) -> List[Any]:

        if criteria is None:
            return data_batch

        filtered: List[Any] = [item for item in data_batch if item is not None]
        return filtered

    def get_stats(self) -> Dict[str, Union[str, int, float]]:

        return {
            "stream_id": self.stream_id,
            "stream_type": self.stream_type,
            "total_processed": self.total_processed,
            "error_count": self.error_count
        }


class SensorStream(DataStream):

    def __init__(self, stream_id: str) -> None:

        super().__init__(stream_id, "Environmental Data")
        self.temp_history: List[float] = []
        self.critical_alerts: int = 0

    def process_batch(self, data_batch: List[Dict[str, Any]]) -> str:

        filtered = self.filter_data(data_batch)
        intervals = {
            "temp": lambda x: x < 18 or x > 27,
            "humidity": lambda x: x < 30 or x > 60,
        }

        for factor, values in filtered.items():
            if factor in intervals:
                self.critical_alerts += sum(
                    1 for v in values if intervals[factor](v)
                )

        if "temp" in filtered:
            self.temp_history.extend(filtered["temp"])

        self.total_processed += len(filtered)

        temp_list = filtered.get("temp", [])
        avg_temp = sum(temp_list) / len(temp_list) if temp_list else 0.0

        return (
            f"Sensor analysis: {len(data_batch)} readings processed, "
            f"avg temp: {avg_temp:.1f}°C"
        )

    def filter_data(
        self,
        data_batch: List[Dict[str, Any]],
        criteria: Optional[str] = "normal-priority"
    ) -> Dict[str, List[float]]:

        factors: Dict[str, List[float]] = {
            "temp": [],
            "humidity": [],
        }

        for reading in data_batch:
            if isinstance(reading, dict):
                for factor in factors:
                    if factor in reading:
                        try:
                            factors[factor].append(float(reading[factor]))
                        except (ValueError, TypeError):
                            self.error_count += 1

        if criteria == "high-priority":
            factors["temp"] = [t for t in factors["temp"] if t < 18 or t > 27]
            factors["humidity"] = [
                h for h in factors["humidity"] if h < 30 or h > 60
            ]

        return factors

    def get_stats(self) -> Dict[str, Union[str, int, float]]:

        stats = super().get_stats()
        stats["critical_alerts"] = self.critical_alerts
        avg: float = 0.0
        if self.temp_history:
            avg = sum(self.temp_history) / len(self.temp_history)
        stats["avg_temperature"] = avg
        return stats


class TransactionStream(DataStream):

    def __init__(self, stream_id: str) -> None:
        super().__init__(stream_id, "Financial Data")
        self.net_flow: int = 0
        self.large_transactions: int = 0

    def process_batch(self, data_batch: List[Any]) -> str:

        filtered = self.filter_data(data_batch)
        operations: int = 0
        flow: int = 0

        for trans in filtered:
            try:
                if "sell" in trans:
                    val = int(trans["sell"])
                    if val > 100:
                        self.large_transactions += 1
                    flow += val
                    operations += 1
            except (ValueError, TypeError):
                self.error_count += 1

            try:
                if "buy" in trans:
                    val = int(trans["buy"])
                    if val > 100:
                        self.large_transactions += 1
                    flow -= val
                    operations += 1
            except (ValueError, TypeError):
                self.error_count += 1

        self.net_flow += flow
        self.total_processed += len(filtered)
        return (
            f"Transaction analysis: {operations} operations, "
            f"net flow: {flow:+} units"
        )

    def filter_data(
        self,
        data_batch: List[Dict[str, float]],
        criteria: Optional[str] = "normal-priority"
    ) -> List[Dict[str, float]]:

        transactions: List[Dict[str, Any]] = [
            t for t in data_batch
            if isinstance(t, dict) and ("buy" in t or "sell" in t)
        ]

        for t in transactions:
            if t.get("buy", 0) < 0:
                t.pop("buy", None)
            if t.get("sell", 0) < 0:
                t.pop("sell", None)

        if criteria == "high-priority":
            for t in transactions:
                if t.get("buy", 0) <= 100:
                    t.pop("buy", None)
                if t.get("sell", 0) <= 100:
                    t.pop("sell", None)

        return transactions

    def get_stats(self) -> Dict[str, Union[str, int, float]]:

        stats = super().get_stats()
        stats["net_flow"] = self.net_flow
        stats["large_transactions"] = self.large_transactions
        return stats


class EventStream(DataStream):

    def __init__(self, stream_id: str) -> None:
        super().__init__(stream_id, "System Events")
        self.events_by_type: Dict[str, int] = {}

    def process_batch(self, data_batch: List[str]) -> str:
        error_count: int = 0
        errors = ["error", "400", "401", "402", "403", "404", "408"]
        filtered = self.filter_data(data_batch)

        for event in filtered:
            e_str = str(event)
            self.events_by_type[e_str] = (
                self.events_by_type.get(e_str, 0) + 1
            )

            if e_str in errors:
                error_count += 1

        self.total_processed += len(filtered)

        msg = f"{error_count} error{'s' if error_count != 1 else ''} detected"

        return f"Event analysis: {len(data_batch)} events, {msg}"

    def filter_data(
        self, data_batch: List[Any],
        criteria: Optional[str] = "normal-priority"
    ) -> List[str]:

        events = ["login", "logout", "sign in", "sign up"]
        errors = ["error", "400", "401", "402", "403", "404", "408"]

        valid = [str(e) for e in data_batch if e in events or e in errors]

        if criteria == "high-priority":
            return [e for e in valid if e in errors]

        return [e for e in valid if e in events]


class StreamProcessor:

    def __init__(self) -> None:
        self.__streams: Dict[DataStream, List[Any]] = {}

    def add_stream(self, stream: DataStream, batches: List[Any]) -> None:
        if isinstance(stream, DataStream):
            if stream in self.__streams:
                self.__streams[stream].extend(batches)
            else:
                self.__streams[stream] = batches

    def process_all_streams(self) -> None:

        for stream, data_batch in self.__streams.items():
            try:
                print(
                    f"Initializing {stream.__class__.__name__[0:-6]} Stream..."
                )
                print(
                    f"Stream ID: {stream.stream_id}, "
                    f"Type: {stream.stream_type}"
                )
                print(
                    "Processing sensor batch: "
                    f"{stream.filter_data(data_batch)}"
                )
                print(f"{stream.process_batch(data_batch)}\n")
                self.add_stream(stream, data_batch)
            except Exception as e:
                print(f"Error in stream {stream.stream_id}: {e}\n")

    def get_all_processed_number(self) -> Dict[DataStream, int]:
        return {
            stream: stream.total_processed for stream in self.__streams
        }

    def high_priority_filtering(self):
        sensor = sum(
            [
                len(s.filter_data(d)) for s, d in self.__streams.items()
                if isinstance(s, SensorStream)
            ]
        )
        transaction = sum(
            [
                len(s.filter_data(d)) for s, d in self.__streams.items()
                if isinstance(s, TransactionStream)
            ]
        )
        return (
            f"Filtered results: {sensor if sensor else None} critical sensor "
            f"alerts, {transaction if transaction else None} large transaction"
        )


def main() -> None:
    print("=== CODE NEXUS - POLYMORPHIC STREAM SYSTEM ===\n")

    processor = StreamProcessor()
    streams: Dict[DataStream, List[Any]] = {
        SensorStream("SENSOR_001"): [
            {"temp": 30}, {"humidity": 74}, {"pressure": 1013}
        ],
        TransactionStream("TRANS_001"): [
            {"buy": 100}, {"sell": 200}, {"buy": 75}, {"kill": 75}
        ],
        EventStream("EVENT_001"): ["error", "404", "login"]
    }

    for stream, data in streams.items():
        processor.add_stream(stream, data)

    processor.process_all_streams()

    print("=== Polymorphic Stream Processing ===")
    print("Processing mixed stream types through unified interface...\n")
    print("Batch 1 Results:")

    for s, n in processor.get_all_processed_number().items():
        print(
            f"- {s.__class__.__name__[0:-6]} data: {n} "
            f"{
                "readings" if isinstance(s, SensorStream) else
                "operations" if isinstance(s, TransactionStream) else "events"
            }"
            " processed"
        )

    print("\nStream filtering active: High-priority data only")
    print(processor.high_priority_filtering())
    print("\nAll streams processed successfully. Nexus throughput optimal.")


if __name__ == "__main__":
    main()
