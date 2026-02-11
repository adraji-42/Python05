from abc import ABC, abstractmethod
from typing import Any, List, Dict, Union, Optional


class DataStream(ABC):
    """
    Abstract base class for data streams with core streaming functionality.
    """

    def __init__(self, stream_id: str, stream_type: str) -> None:
        """
        Initialize the data stream.

        Args:
            stream_id: Unique identifier for the stream
            stream_type: Type description of the stream
        """
        self.stream_id = stream_id
        self.stream_type = stream_type
        self.total_processed = 0
        self.error_count = 0

    @abstractmethod
    def process_batch(self, data_batch: List[Any]) -> str:
        """
        Process a batch of data.

        Args:
            data_batch: List of data items to process

        Returns:
            String describing the processing results
        """
        pass

    def filter_data(
        self,
        data_batch: List[Any],
        criteria: Optional[str] = None
    ) -> List[Any]:
        """
        Filter data based on criteria.

        Args:
            data_batch: List of data items to filter
            criteria: Optional filtering criteria

        Returns:
            Filtered list of data items
        """
        if criteria is None:
            return data_batch

        filtered = [item for item in data_batch if item is not None]
        return filtered

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        """
        Return stream statistics.

        Returns:
            Dictionary containing stream statistics
        """
        return {
            "stream_id": self.stream_id,
            "stream_type": self.stream_type,
            "total_processed": self.total_processed,
            "error_count": self.error_count
        }


class SensorStream(DataStream):
    """Specialized stream for sensor/environmental data."""

    def __init__(self, stream_id: str) -> None:
        """
        Initialize sensor stream.

        Args:
            stream_id: Unique identifier for the sensor stream
        """
        super().__init__(stream_id, "Environmental Data")
        self.temp_history: List[float] = []
        self.humidity_history: List[float] = []
        self.critical_alerts = 0

    def process_batch(self, data_batch: List[Any]) -> str:
        """
        Process a batch of sensor data.

        Args:
            data_batch: List of sensor readings

        Returns:
            Analysis results as a string
        """
        try:
            temps: List[float] = []
            humidities: List[float] = []
            filtered = self.filter_data(data_batch)

            for reading in filtered:
                if isinstance(reading, dict):
                    if "temp" in reading:
                        try:
                            temp = float(reading["temp"])
                            temps.append(temp)
                            self.temp_history.append(temp)

                            if temp < 18 or temp > 27:
                                self.critical_alerts += 1
                        except (ValueError, TypeError):
                            self.error_count += 1

                    if "humidity" in reading:
                        try:
                            humidity = float(reading["humidity"])
                            humidities.append(humidity)
                            self.humidity_history.append(humidity)

                            if humidity < 30 or humidity > 60:
                                self.critical_alerts += 1
                        except (ValueError, TypeError):
                            self.error_count += 1

            self.total_processed += len(data_batch)
            avg_temp = sum(temps) / len(temps) if temps else 0.0
            avg_humidity = (
                sum(humidities) / len(humidities) if humidities else 0.0
            )
            analysis_parts: List[str] = []
            analysis_parts.append(f"{len(filtered)} readings processed")

            if temps:
                analysis_parts.append(f"avg temp: {avg_temp:.1f}°C")
            if humidities:
                analysis_parts.append(f"avg humidity: {avg_humidity:.1f}%")

            return f"Sensor analysis: {', '.join(analysis_parts)}"
        except Exception:
            self.error_count += 1
            return "Sensor processing error"

    def filter_data(
        self,
        data_batch: List[Any],
        criteria: Optional[str] = None
    ) -> List[Any]:
        """
        Filter sensor data based on criteria.

        Args:
            data_batch: List of sensor readings
            criteria: Filtering criteria

        Returns:
            Filtered sensor data
        """
        if criteria == "high-priority":
            filtered: List[Any] = []
            for reading in data_batch:
                if isinstance(reading, dict):
                    is_critical: bool = False

                    if "temp" in reading:
                        temp = float(reading["temp"])
                        if temp < 18 or temp > 27:
                            is_critical = True

                    if "humidity" in reading:
                        humidity = float(reading["humidity"])
                        if humidity < 30 or humidity > 60:
                            is_critical = True

                    if is_critical:
                        filtered.append(reading)

            return filtered

        return [
            reading for reading in data_batch
            if isinstance(reading, dict)
            and (
                "temp" in reading
                or "humidity" in reading
                or "pressure" in reading
            )
        ]

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        """Return sensor stream statistics.

        Returns:
            Dictionary with sensor-specific statistics
        """
        stats: Dict[str, Union[str, int, float]] = super().get_stats()
        stats["critical_alerts"] = self.critical_alerts

        avg_temp = 0.0
        if self.temp_history:
            avg_temp = sum(self.temp_history) / len(self.temp_history)
        stats["avg_temperature"] = avg_temp

        avg_humidity = 0.0
        if self.humidity_history:
            avg_humidity = (
                sum(self.humidity_history) / len(self.humidity_history)
            )
        stats["avg_humidity"] = avg_humidity

        return stats


class TransactionStream(DataStream):
    """Specialized stream for financial transaction data."""

    def __init__(self, stream_id: str) -> None:
        """
        Initialize transaction stream.

        Args:
            stream_id: Unique identifier for the transaction stream
        """
        super().__init__(stream_id, "Financial Data")
        self.net_flow = 0
        self.large_transactions = 0

    def process_batch(self, data_batch: List[Any]) -> str:
        """
        Process a batch of transaction data.

        Args:
            data_batch: List of transactions

        Returns:
            Analysis results as a string
        """
        try:
            flow = 0
            operations = 0
            filtered = self.filter_data(data_batch)

            for transaction in filtered:
                if isinstance(transaction, dict):
                    try:
                        if "buy" in transaction:
                            amount = int(transaction["buy"])
                            if amount > 0:
                                flow -= amount
                                operations += 1
                                if amount > 100:
                                    self.large_transactions += 1

                        if "sell" in transaction:
                            amount = int(transaction["sell"])
                            if amount > 0:
                                flow += amount
                                operations += 1
                                if amount > 100:
                                    self.large_transactions += 1
                    except (ValueError, TypeError):
                        self.error_count += 1

            self.net_flow += flow
            self.total_processed += len(data_batch)

            return (
                f"Transaction analysis: {operations} operations, "
                f"net flow: {flow:+} units"
            )
        except Exception:
            self.error_count += 1
            return "Transaction processing error"

    def filter_data(
        self,
        data_batch: List[Any],
        criteria: Optional[str] = None
    ) -> List[Any]:
        """
        Filter transaction data based on criteria.

        Args:
            data_batch: List of transactions
            criteria: Filtering criteria

        Returns:
            Filtered transaction data
        """
        valid_transactions: List[Any] = [
            trans for trans in data_batch
            if isinstance(trans, dict) and ("buy" in trans or "sell" in trans)
        ]

        if criteria == "high-priority":
            filtered: List[Any] = [
                trans for trans in valid_transactions
                if (trans.get("buy", 0) > 100 or trans.get("sell", 0) > 100)
            ]
            return filtered

        return valid_transactions

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        """
        Return transaction stream statistics.

        Returns:
            Dictionary with transaction-specific statistics
        """
        stats: Dict[str, Union[str, int, float]] = super().get_stats()
        stats["net_flow"] = self.net_flow
        stats["large_transactions"] = self.large_transactions
        return stats


class EventStream(DataStream):
    """Specialized stream for system event data."""

    def __init__(self, stream_id: str) -> None:
        """
        Initialize event stream.

        Args:
            stream_id: Unique identifier for the event stream
        """
        super().__init__(stream_id, "System Events")
        self.events_by_type: Dict[str, int] = {}

    def process_batch(self, data_batch: List[Any]) -> str:
        """
        Process a batch of event data.

        Args:
            data_batch: List of events

        Returns:
            Analysis results as a string
        """
        try:
            error_count = 0
            error_types = ["error", "400", "401", "403", "404", "500"]

            for event in data_batch:
                event_str = str(event)
                self.events_by_type[event_str] = (
                    self.events_by_type.get(event_str, 0) + 1
                )

                if event_str in error_types:
                    error_count += 1

            self.total_processed += len(data_batch)

            error_msg = (
                f"{error_count} error{'s' if error_count != 1 else ''} "
                "detected"
            )
            return f"Event analysis: {len(data_batch)} events, {error_msg}"
        except Exception:
            self.error_count += 1
            return "Event processing error"

    def filter_data(
        self,
        data_batch: List[Any],
        criteria: Optional[str] = None
    ) -> List[Any]:
        """Filter event data based on criteria.

        Args:
            data_batch: List of events
            criteria: Filtering criteria

        Returns:
            Filtered event data
        """
        error_types: List[str] = ["error", "400", "401", "403", "404", "500"]

        if criteria == "high-priority":
            return [event for event in data_batch if str(event) in error_types]

        return data_batch

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        """
        Return event stream statistics.

        Returns:
            Dictionary with event-specific statistics
        """
        stats: Dict[str, Union[str, int, float]] = super().get_stats()
        stats["events_by_type"] = str(self.events_by_type)
        return stats


class StreamProcessor:
    """Manager class that handles multiple stream types polymorphically."""

    def __init__(self) -> None:
        """Initialize the stream processor."""
        self.streams: List[DataStream] = []
        self.stream_data: Dict[DataStream, List[Any]] = {}

    def add_stream(self, stream: DataStream, data_batch: List[Any]) -> None:
        """
        Add a stream with its data to the processor.

        Args:
            stream: DataStream instance to add
            data_batch: Data to associate with the stream
        """
        if isinstance(stream, DataStream):
            if stream not in self.streams:
                self.streams.append(stream)
            self.stream_data[stream] = data_batch

    def process_all_streams(self) -> None:
        """Process all streams polymorphically."""
        for stream in self.streams:
            if stream in self.stream_data:
                try:
                    data_batch: List[Any] = self.stream_data[stream]

                    stream_type_name = stream.__class__.__name__[:-6]

                    print(f"Initializing {stream_type_name} Stream...")
                    print(
                        f"Stream ID: {stream.stream_id}, "
                        f"Type: {stream.stream_type}"
                    )
                    print(
                        "Processing sensor batch: "
                        f"{stream.filter_data(data_batch)}"
                    )
                    print(f"{stream.process_batch(data_batch)}\n")
                except Exception as e:
                    print(f"Error processing stream {stream.stream_id}: {e}\n")

    def get_all_stats(self) -> Dict[DataStream, int]:
        """
        Get processing statistics for all streams.

        Returns:
            Dictionary mapping streams to their processed count
        """
        return {stream: stream.total_processed for stream in self.streams}

    def filter_high_priority(self) -> str:
        """
        Filter all streams for high-priority data.

        Returns:
            Summary of filtered results
        """
        sensor_alerts = 0
        large_transactions = 0

        for stream in self.streams:
            if stream in self.stream_data:
                data: List[Any] = self.stream_data[stream]
                filtered: List[Any] = stream.filter_data(data, "high-priority")

                if isinstance(stream, SensorStream):
                    sensor_alerts += len(filtered)
                elif isinstance(stream, TransactionStream):
                    large_transactions += len(filtered)

        return (
            f"Filtered results: {sensor_alerts} critical sensor alerts, "
            f"{large_transactions} large transaction"
        )


def main() -> None:
    """Main function demonstrating the polymorphic stream system."""
    print("=== CODE NEXUS - POLYMORPHIC STREAM SYSTEM ===\n")

    processor = StreamProcessor()

    processor.add_stream(
        SensorStream("SENSOR_001"),
        [{"temp": 32.5}, {"humidity": 65}, {"pressure": 1013}]
    )
    processor.add_stream(
        TransactionStream("TRANS_001"),
        [{"buy": 100}, {"sell": 150}, {"buy": 75}, None]
    )
    processor.add_stream(
        EventStream("EVENT_001"),
        ["login", "error", "logout"]
    )

    processor.process_all_streams()

    print("=== Polymorphic Stream Processing ===")
    print("Processing mixed stream types through unified interface...\n")
    print("Batch 1 Results:")

    for stream, count in processor.get_all_stats().items():
        data_type = (
            "readings" if isinstance(stream, SensorStream) else
            "operations" if isinstance(stream, TransactionStream) else
            "events"
        )
        stream_name = stream.__class__.__name__[:-6]
        print(f"- {stream_name} data: {count} {data_type} processed")

    print("\nStream filtering active: High-priority data only")
    print(processor.filter_high_priority())
    print("\nAll streams processed successfully. Nexus throughput optimal.")


if __name__ == "__main__":
    main()
