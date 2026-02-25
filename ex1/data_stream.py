from abc import ABC, abstractmethod
from typing import Any, List, Dict, Union, Optional


class DataStream(ABC):
    """Abstract base class for data streams with core streaming functionality.

    This class provides the blueprint for processing, filtering, and
    monitoring statistics for different data stream implementations.
    """

    def __init__(self, stream_id: str, stream_type: str) -> None:
        """Initializes the data stream with basic tracking metrics.

        Args:
            stream_id: Unique identifier for the stream.
            stream_type: Type description of the stream.
        """
        self.stream_id = stream_id
        self.stream_type = stream_type
        self.total_processed = 0
        self.error_count = 0

    @abstractmethod
    def process_batch(self, data_batch: List[Any]) -> str:
        """Process a batch of data items.

        Args:
            data_batch: List of data items to process.

        Returns:
            str: A summary string of the processing results.
        """
        pass

    def filter_data(
        self,
        data_batch: List[Any],
        criteria: Optional[str] = None
    ) -> List[Any]:
        """Filters data batch based on a given criteria.

        Args:
            data_batch: List of data items to filter.
            criteria: Optional filtering criteria string.

        Returns:
            List[Any]: Filtered list of items.
        """
        if criteria is None:
            return data_batch

        filtered = [item for item in data_batch if item is not None]
        return filtered

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        """Calculates and returns the stream statistics.

        Returns:
            Dict[str, Union[str, int, float]]: Dictionary containing IDs,
                types, and processing counts.
        """
        return {
            "stream_id": self.stream_id,
            "stream_type": self.stream_type,
            "total_processed": self.total_processed,
            "error_count": self.error_count
        }


class SensorStream(DataStream):
    """Specialized stream for environmental sensor data.

    Tracks temperature, humidity, and generates alerts for
    out-of-range values.
    """

    def __init__(self, stream_id: str) -> None:
        """Initializes sensor-specific storage.

        Args:
            stream_id: Unique identifier for the sensor stream.
        """
        super().__init__(stream_id, "Environmental Data")
        self.temp_history: List[float] = []
        self.humidity_history: List[float] = []
        self.critical_alerts = 0

    def process_batch(self, data_batch: List[Any]) -> str:
        """Analyzes sensor readings for environmental thresholds.

        Args:
            data_batch: List of sensor dictionaries.

        Returns:
            str: Summary of readings including average temp and humidity.
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
                analysis_parts.append(
                    f"avg humidity: {avg_humidity:.1f}%"
                )

            return f"Sensor analysis: {', '.join(analysis_parts)}"
        except Exception:
            self.error_count += 1
            return "Sensor processing error"

    def filter_data(
        self,
        data_batch: List[Any],
        criteria: Optional[str] = None
    ) -> List[Any]:
        """Filters sensor data for critical ranges.

        Args:
            data_batch: List of readings.
            criteria: Priority string.

        Returns:
            List[Any]: High-priority or valid sensor readings.
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
        """Extends base stats with sensor averages and alerts.

        Returns:
            Dict[str, Union[str, int, float]]: Full sensor statistics.
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
    """Specialized stream for financial transaction processing.

    Monitors net cash flow and flags large volume transactions.
    """

    def __init__(self, stream_id: str) -> None:
        """Initializes financial tracking variables.

        Args:
            stream_id: Unique identifier for the transaction stream.
        """
        super().__init__(stream_id, "Financial Data")
        self.net_flow = 0
        self.large_transactions = 0

    def process_batch(self, data_batch: List[Any]) -> str:
        """Processes buy and sell operations.

        Args:
            data_batch: List of transaction dictionaries.

        Returns:
            str: Net flow analysis summary.
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
        """Filters transactions based on amount thresholds.

        Args:
            data_batch: Raw transactions.
            criteria: High-priority flag.

        Returns:
            List[Any]: Filtered transaction list.
        """
        valid_transactions: List[Any] = [
            trans for trans in data_batch
            if isinstance(trans, dict)
            and ("buy" in trans or "sell" in trans)
        ]

        if criteria == "high-priority":
            filtered: List[Any] = [
                trans for trans in valid_transactions
                if (
                    trans.get("buy", 0) > 100
                    or trans.get("sell", 0) > 100
                )
            ]
            return filtered

        return valid_transactions

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        """Returns flow and transaction volume statistics.

        Returns:
            Dict[str, Union[str, int, float]]: Financial stats.
        """
        stats: Dict[str, Union[str, int, float]] = super().get_stats()
        stats["net_flow"] = self.net_flow
        stats["large_transactions"] = self.large_transactions
        return stats


class EventStream(DataStream):
    """Specialized stream for system event logs.

    Identifies and counts system events by type and severity.
    """

    def __init__(self, stream_id: str) -> None:
        """Initializes event type mapping.

        Args:
            stream_id: Unique identifier for the event stream.
        """
        super().__init__(stream_id, "System Events")
        self.events_by_type: Dict[str, int] = {}

    def process_batch(self, data_batch: List[Any]) -> str:
        """Categorizes system events and error flags.

        Args:
            data_batch: List of event labels.

        Returns:
            str: Event count and error detection summary.
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
                f"{error_count} error"
                f"{'s' if error_count != 1 else ''} detected"
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
        """Filters events to show only error types.

        Args:
            data_batch: Raw event data.
            criteria: Filter priority.

        Returns:
            List[Any]: Filtered events.
        """
        error_types: List[str] = [
            "error", "400", "401", "403", "404", "500"
        ]

        if criteria == "high-priority":
            return [
                event for event in data_batch
                if str(event) in error_types
            ]

        return data_batch

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        """Summarizes events by their frequency.

        Returns:
            Dict[str, Union[str, int, float]]: Frequency stats.
        """
        stats: Dict[str, Union[str, int, float]] = super().get_stats()
        stats["events_by_type"] = str(self.events_by_type)
        return stats


class StreamProcessor:
    """Manager class that handles multiple stream types polymorphically.

    Aggregates different streams and orchestrates their processing cycles.
    """

    def __init__(self) -> None:
        """Initializes internal stream registries."""
        self.__streams: List[DataStream] = []
        self.__stream_data: Dict[DataStream, List[Any]] = {}

    def add_stream(
        self, stream: DataStream, data_batch: List[Any]
    ) -> None:
        """Registers a stream and its associated data.

        Args:
            stream: DataStream instance.
            data_batch: The initial data batch for this stream.
        """
        if isinstance(stream, DataStream):
            if stream not in self.__streams:
                self.__streams.append(stream)
            self.__stream_data[stream] = data_batch

    def process_all_streams(self) -> None:
        """Executes processing on all registered streams.

        Uses polymorphic calls to process_batch based on stream type.
        """
        for stream in self.__streams:
            if stream in self.__stream_data:
                try:
                    data_batch: List[Any] = self.__stream_data[stream]
                    stream_type_name = (
                        stream.__class__.__name__.removesuffix("Stream")
                    )

                    print(f"Initializing {stream_type_name} Stream...")
                    print(
                        f"Stream ID: {stream.stream_id}, "
                        f"Type: {stream.stream_type}"
                    )
                    print(
                        f"Processing {stream_type_name.lower()} batch: "
                        f"{stream.filter_data(data_batch)}"
                    )
                    print(f"{stream.process_batch(data_batch)}\n")
                except Exception as e:
                    print(
                        f"Error processing stream "
                        f"{stream.stream_id}: {e}\n"
                    )

    def get_all_stats(self) -> Dict[DataStream, int]:
        """Aggregates processing counts from all streams.

        Returns:
            Dict[DataStream, int]: Map of streams to total items processed.
        """
        return {
            stream: stream.total_processed for stream in self.__streams
        }

    def filter_high_priority(self) -> str:
        """Collects high-priority alerts across all stream types.

        Returns:
            str: Summary of critical sensor alerts and large transactions.
        """
        sensor_alerts = 0
        large_transactions = 0

        for stream in self.__streams:
            if stream in self.__stream_data:
                data: List[Any] = self.__stream_data[stream]
                filtered: List[Any] = stream.filter_data(
                    data, "high-priority"
                )

                if isinstance(stream, SensorStream):
                    sensor_alerts += len(filtered)
                elif isinstance(stream, TransactionStream):
                    large_transactions += len(filtered)

        t_suffix = "s" if large_transactions != 1 else ""
        return (
            f"Filtered results: {sensor_alerts} critical sensor alerts, "
            f"{large_transactions} large transaction{t_suffix}"
        )


def main() -> None:
    """Entry point for the stream system demo."""
    print("=== CODE NEXUS - POLYMORPHIC STREAM SYSTEM ===\n")

    processor = StreamProcessor()
    streams = [
        (
            SensorStream("SENSOR_001"),
            [{"temp": 32.5}, {"humidity": 65}, {"pressure": 1013}]
        ),
        (
            TransactionStream("TRANS_001"),
            [{"buy": 100}, {"sell": 150}, {"buy": 75}, None]
        ),
        (EventStream("EVENT_001"), ["login", "error", "logout"])
    ]

    for stream, data in streams:
        processor.add_stream(stream, data)

    try:
        processor.process_all_streams()
    except Exception as e:
        print(f"Unexpected Error {e}")

    print("=== Polymorphic Stream Processing ===")
    print("Processing mixed stream types through unified interface...\n")
    print("Batch 1 Results:")

    try:
        for stream, count in processor.get_all_stats().items():
            data_type = (
                "readings" if isinstance(stream, SensorStream) else
                "operations" if isinstance(stream, TransactionStream)
                else "events"
            )
            stream_name = stream.__class__.__name__.removesuffix("Stream")
            print(f"- {stream_name} data: {count} {data_type} processed")
    except Exception as e:
        print(f"Unexpected Error {e}")

    print("\nStream filtering active: High-priority data only")
    print(processor.filter_high_priority())
    print("\nAll streams processed successfully. Nexus throughput optimal.")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"Unexpected Error {e}")
