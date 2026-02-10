from abc import ABC, abstractmethod
from typing import Any, Union, List


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
    def process(self, data: Union[int | List[int]]) -> str:
        if not self.validate(data):
            raise ValueError(
                "Invalid numeric data: must be numeric or list of numbers"
            )

        data_list = data if isinstance(data, list) else [data]
        count = len(data_list)
        total = sum(data_list)
        avg = total / count

        return super().format_output(
            f"Processed {count:.0f} numeric values, "
            f"sum={total:.0f}, avg={avg:.1f}"
        )

    def validate(self, data: Union[int | List[int]]) -> bool:
        if isinstance(data, (int, float)):
            return True

        return (
            isinstance(data, list)
            and len(data) > 0
            and all(isinstance(x, (int, float)) for x in data)
        )


class TextProcessor(DataProcessor):

    def process(self, data: str) -> str:
        if not self.validate(data):
            raise ValueError("Invalid text data: must be a string")

        return super().format_output(
            f"Processed text: {len(data)} "
            f"characters, {len(data.split())} words"
        )

    def validate(self, data: str) -> bool:
        return isinstance(data, str)


class LogProcessor(DataProcessor):

    def process(self, data: str) -> str:
        if not self.validate(data):
            raise ValueError("Invalid log format. Expected 'LEVEL: MESSAGE'")

        level, message = map(str.strip, data.split(':'))
        log_type = "ALERT" if level.upper() in ("ERROR", "WARNING") else level

        return super().format_output(
            f"[{log_type}] {level} level detected: {message}"
        )

    def validate(self, data: str) -> bool:
        return isinstance(data, str) and data.count(':') == 1


def main() -> None:
    print("=== CODE NEXUS - DATA PROCESSOR FOUNDATION ===")

    processors = [
        (NumericProcessor(), [1, 2, 3, 4, 5], "Numeric"),
        (TextProcessor(), "Hello Nexus World", "Text"),
        (LogProcessor(), "ERROR: Connection timeout", "Log")
    ]

    try:
        for proc, data, name in processors:
            print(f"\nInitializing {name} Processor...")
            try:
                print(f"Processing data: {data}")
                output = proc.process(data)
            except ValueError as e:
                print(f"Error: {e}")
            except Exception as e:
                print(f"Unexpected Error: {e}")
            else:
                print(f"Validation: {name} data verified")
                print(output)
    except Exception as e:
        print(f"Unexpected Error: {e}")

    print("\n=== Polymorphic Processing Demo ===")
    multiple_data = [
        (NumericProcessor(), [1, 21, 31]),
        (TextProcessor(), "This project is perfect"),
        (
            LogProcessor(),
            "INFO: You must press the outstanding flag to save your life"
        )
    ]

    try:
        for i, (proc, data) in enumerate(multiple_data, 1):
            try:
                result = proc.process(data)
                print(f"Result {i}: {result}")
            except ValueError as e:
                print(f"Item {i} failed: {e}")
            except Exception as e:
                print(f"In Item {i}: Unexpected Error: {e}")
    except Exception as e:
        print(f"Unexpected Error: {e}")


if __name__ == "__main__":
    main()
