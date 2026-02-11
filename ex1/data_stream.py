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
        self, data_batch: List[Any], criteria: Optional[str] = None
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

        filtered = self.filter_data(data_batch, None)
        intervals = {
            "temp": lambda x: x < 18 or x > 27,
            "humidity": lambda x: x < 30 or x > 60,
            "pressure": lambda x: x < 900 or x > 1100
        }

        for factor, values in filtered.items():
            if factor in intervals:
                self.critical_alerts += sum(
                    1 for v in values if intervals[factor](v)
                )

        if "temp" in filtered:
            self.temp_history.extend(filtered["temp"])

        self.total_processed += len(data_batch)

        temp_list = filtered.get("temp", [])
        avg_temp = sum(temp_list) / len(temp_list) if temp_list else 0.0

        return (
            f"Sensor analysis: {len(data_batch)} readings processed, "
            f"avg temp: {avg_temp:.1f}°C"
        )

    def filter_data(
        self,
        data_batch: List[Dict[str, Any]],
        criteria: Optional[str] = None
    ) -> Dict[str, List[float]]:

        factors: Dict[str, List[float]] = {
            "temp": [],
            "humidity": [],
            "pressure": []
        }

        for reading in data_batch:
            if isinstance(reading, dict):
                for factor in factors:
                    if factor in reading:
                        try:
                            factors[factor].append(float(reading[factor]))
                        except (ValueError, TypeError):
                            self.critical_alerts += 1

        if criteria == "high-priority":
            factors["temp"] = [t for t in factors["temp"] if t < 18 or t > 27]
            factors["humidity"] = [
                h for h in factors["humidity"] if h < 30 or h > 60
            ]
            factors["pressure"] = [
                p for p in factors["pressure"] if p < 900 or p > 1100
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

        filtered = self.filter_data(data_batch, "high-priority")
        operations: int = 0
        flow: int = 0

        for trans in filtered:
            try:
                if "sell" in trans:
                    price = int(trans["sell"])
                    if price > 100:
                        self.large_transactions += 1
                    flow += price
                    operations += 1
            except (ValueError, TypeError):
                self.error_count += 1

            try:
                if "buy" in trans:
                    price = int(trans["buy"])
                    if price > 100:
                        self.large_transactions += 1
                    flow -= price
                    operations += 1
            except (ValueError, TypeError):
                self.error_count += 1

        self.net_flow += flow
        self.total_processed += len(data_batch)
        return (
            f"Transaction analysis: {operations} operations, "
            f"net flow: {flow:+} units"
        )

    def filter_data(
        self,
        data_batch: List[Dict[str, float]],
        criteria: Optional[str] = None
    ) -> List[Dict[str, float]]:

        transactions: List[Dict[str, Any]] = [
            t for t in data_batch
            if isinstance(t, dict) and ("buy" in t or "sell" in t)
        ]

        if criteria == "high-priority":
            for t in transactions:
                if t.get("buy", 0) < 0:
                    t.pop("buy", None)
                if t.get("sell", 0) < 0:
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
        filtered = self.filter_data(data_batch, "high-priority")

        for event in filtered:
            event_str = str(event)
            self.events_by_type[event_str] = (
                self.events_by_type.get(event_str, 0) + 1
            )
            if event_str in errors:
                error_count += 1

        self.total_processed += len(data_batch)
        err_msg: str = ""
        if error_count == 1:
            err_msg = ", 1 error detected"
        elif error_count > 1:
            err_msg = f", {error_count} errors detected"

        return f"Event analysis: {len(data_batch)} events{err_msg}"

    def filter_data(
        self, data_batch: List[str], criteria: Optional[str] = None
    ) -> List[str]:

        events = ["login", "logout", "sign in", "sign up"]
        errors = ["error", "400", "401", "402", "403", "404", "408"]

        valid_events = [str(e) for e in data_batch if e is not None]

        if criteria == "high-priority":
            return [
                e for e in valid_events
                if e in events or e in errors
            ]

        return [e for e in valid_events if e in events]


class StreamProcessor:

    def __init__(self) -> None:
        self.__streams: Dict[DataStream, List[Any]] = {}

    def add_stream(self, stream: DataStream, batches: List[Any]) -> None:
        if isinstance(stream, DataStream):
            if stream in self.__streams:
                self.__streams[stream].extend(batches)
            else:
                self.__streams[stream] = batches

    def process_all_streams(self) -> List[str]:
        results: List[str] = []

        for i, stream, batches in enumerate(self.__streams.items()):
            if i < len(batches):
                try:
                    results.append(stream.process_batch(batches))
                except Exception:
                    results.append(f"Error in stream {stream.stream_id}")
        return results

    def get_all_stats(self) -> List[Dict[str, Union[str, int, float]]]:
        return [s.get_stats() for s in self.__streams]


def main() -> None:
    print("=== CODE NEXUS - POLYMORPHIC STREAM SYSTEM ===\n")

    processor = StreamProcessor()
    streams: List[DataStream] = [
        SensorStream("SENSOR_001"),
        TransactionStream("TRANS_001"),
        EventStream("EVENT_001")
    ]
    all_data_batches: List[List[Any]] = [
        [{"temp": 30}, {"humidity": 74}, {"pressure": 1013}],
        [{"buy": 100}, {"sell": 250}],
        ["error", "404", "login"]
    ]

    for stream in streams:
        processor.add_stream(stream, all_data_batches)

    print("Running Polymorphic Processing through StreamProcessor...")
    try:
        results = processor.process_all_streams(all_data_batches)
    except Exception as e:
        print(f"Unexpected Error: {e}")

    for res in results:
        print(f"[*] {res}")

    print("\nFinal Statistics Summary:")
    for stats in processor.get_all_stats():
        print(
            f"Stream {stats['stream_id']} ({stats['stream_type']}): "
            f"Processed: {stats['total_processed']}"
        )


if __name__ == "__main__":
    main()
