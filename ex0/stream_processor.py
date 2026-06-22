from abc import ABC, abstractmethod
from typing import Any, Union, List


class DataProcessor(ABC):
    """Abstract base class for data processing units.

    This class defines the interface for processing and validating different
    types of data inputs.
    """

    @abstractmethod
    def process(self, data: Any) -> str:
        """Processes the input data and returns a formatted string.

        Args:
            data: The input data to be processed.

        Returns:
            str: The processed result as a string.
        """
        pass

    @abstractmethod
    def validate(self, data: Any) -> bool:
        """Validates the input data format and type.

        Args:
            data: The input data to be checked.

        Returns:
            bool: True if data is valid, False otherwise.
        """
        pass

    def format_output(self, result: str) -> str:
        """Standardizes the output format for all processors.

        Args:
            result: The raw string result from processing.

        Returns:
            str: A prefixed string with the processing result.
        """
        return f"Output: {result}"


class NumericProcessor(DataProcessor):
    """Processor for numerical data including single integers or lists.

    Handles calculations like sum and average for the provided numeric inputs.
    """

    def process(self, data: Union[int, List[int]]) -> str:
        """Calculates statistics for numeric data.

        Args:
            data: An integer or a list of integers.

        Returns:
            str: A summary of count, sum, and average.

        Raises:
            ValueError: If the validation of numeric data fails.
        """
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

    def validate(self, data: Union[int, List[int]]) -> bool:
        """Checks if the data is a number or a list of numbers.

        Args:
            data: The data to validate.

        Returns:
            bool: True if valid numeric/list, False otherwise.
        """
        if isinstance(data, (int, float)):
            return True

        return (
            isinstance(data, list)
            and len(data) > 0
            and all(isinstance(x, (int, float)) for x in data)
        )


class TextProcessor(DataProcessor):
    """Processor for string-based data.

    Analyzes text to provide character and word counts.
    """

    def process(self, data: str) -> str:
        """Analyzes the given text string.

        Args:
            data: The string to process.

        Returns:
            str: A count of characters and words.

        Raises:
            ValueError: If input is not a string.
        """
        if not self.validate(data):
            raise ValueError("Invalid text data: must be a string")

        return super().format_output(
            f"Processed text: {len(data)} "
            f"characters, {len(data.split())} words"
        )

    def validate(self, data: str) -> bool:
        """Checks if the data is a string.

        Args:
            data: The data to validate.

        Returns:
            bool: True if it is a string, False otherwise.
        """
        return isinstance(data, str)


class LogProcessor(DataProcessor):
    """Processor for log messages formatted with colons.

    Categorizes logs based on severity levels.
    """

    def process(self, data: str) -> str:
        """Parses log strings into levels and messages.

        Args:
            data: A string in 'LEVEL: MESSAGE' format.

        Returns:
            str: A categorized log entry.

        Raises:
            ValueError: If the log format is incorrect.
        """
        if not self.validate(data):
            raise ValueError("Invalid log format. Expected 'LEVEL: MESSAGE'")

        level, message = map(str.strip, data.split(':'))
        log_type = "ALERT" if level.upper() in ("ERROR", "WARNING") else level

        return super().format_output(
            f"[{log_type}] {level} level detected: {message}"
        )

    def validate(self, data: str) -> bool:
        """Validates the colon-separated log format.

        Args:
            data: The log string to validate.

        Returns:
            bool: True if the format matches, False otherwise.
        """
        return isinstance(data, str) and data.count(':') == 1


def main() -> None:
    """Entry point for the data processing application.

    Demonstrates polymorphic behavior using different processor types.
    """
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
    try:
        main()
    except Exception as e:
        print(f"Unexpected Error: {e}")
