from abc import ABC, abstractmethod
from typing import Any, List, Tuple


class DataProcessor(ABC):

    @abstractmethod
    def process(self, data: Any) -> str:
        pass

    @abstractmethod
    def validate(self, data: Any) -> bool:
        pass

    def format_output(self, result: str) -> str:
        return f"Output: {result}"


class NumericProcessor(DataProcessor):

    def process(self, data: Any) -> str:
        if not self.validate(data):
            raise ValueError(
                "Invalid numeric data, the data muste be all numeric"
            )

        count = len(data)
        total = sum(data)
        avg = total / count if count > 0 else 0

        return super().format_output(
            f"Processed {count} numeric values, sum={total}, avg={avg:.1f}"
        )

    def validate(self, data: Any) -> bool:
        if isinstance(data, (int, float)):
            return True

        if data and isinstance(data, list):
            if all(isinstance(x, (int, float)) for x in data):
                return True

        return False


class TextProcessor(DataProcessor):

    def process(self, data: Any) -> str:
        if self.validate(data):
            raise ValueError("Invalid text data, the data muste be string")

        char_num = len(data)
        words_num = len(data.split())

        return super().format_output(
            f"Processed text: {char_num} characters, {words_num} words"
        )

    def validate(self, data: Any) -> bool:
        return isinstance(data, str)


class LogProcessor(DataProcessor):

    def process(self, data: Any) -> str:
        if not self.validate(data):
            raise ValueError(
                "Invalid log data, the data muste be with this formul:\n"
                "\tALERT: level detected"
            )

        alert, level = map(str.strip, data.split(':'))
        log_type = "ALERT" if alert.upper() in ("ERROR", "WARNING") else alert

        return super().format_output(
            f"[{log_type}] {alert} level detected: {level}"
        )

    def validate(self, data: Any) -> bool:
        return isinstance(data, str) and data.count(':') != 1 in data


def main() -> None:

    print("=== CODE NEXUS - DATA PROCESSOR FOUNDATION ===")

    numeric_data: List[int] = [1, 2, 3, 4, 5]

    try:
        print("\nInitializing Numeric Processor...")
        print(f"Processing data: {numeric_data}")

        output = NumericProcessor.process(numeric_data)
    except ValueError as e:
        print(f"Validation: {e}")
    except Exception as e:
        print(f"Unexpected Error: {e}")
    else:
        print("Validation: Numeric data verified")
        print(output)

    str_data: str = "Hello Nexus World"

    try:
        print("\nInitializing text Processor...")
        print(f"Processing data: {str_data}")

        output = TextProcessor.process(str_data)
    except ValueError as e:
        print(f"Validation: {e}")
    except Exception as e:
        print(f"Unexpected Error: {e}")
    else:
        print("Validation: Text data verified")
        print(output)

    log_data: str = "ERROR: Connection timeout"

    try:
        print("\nInitializing Log Processor...")
        print(f"Processing data: {log_data}")

        output = LogProcessor.process(log_data)
    except ValueError as e:
        print(f"Validation: {e}")
    except Exception as e:
        print(f"Unexpected Error: {e}")
    else:
        print("Validation: Numeric data verified")
        print(output)

    print("\n=== Polymorphic Processing Demo ===")

    multiple_data: List[Tuple[Any: DataProcessor]] = [
        ([1, 21, 31], NumericProcessor),
        ("This project is perfect", TextProcessor),
        (
            "WARNING: You must press the outstanding flag to save your life",
            LogProcessor
        )
    ]

    try:
        print("\nProcessing multiple data types through same interface...")
        for i, data, processor in enumerate(multiple_data):
            output = processor(data)
            print(f"Result {i}: {output}")
    except ValueError as e:
        print(f"Bad Data: {e}")
    except Exception as e:
        print(f"Unexpected Error: {e}")

    print("Foundation systems online. Nexus ready for advanced streams")


if __name__ == "__main__":
    main()
