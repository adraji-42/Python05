import sys
import time
import random
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Protocol, Union, runtime_checkable


class Base26Converter:
    """A converter for base-10 to base-26 (alphabetical) and vice versa."""

    def __init__(self) -> None:
        self.alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        self.base = len(self.alphabet)

    def encode(self, number: int) -> str:
        """Encode an integer to a base-26 string.

        Args:
            number: The integer to encode.

        Returns:
            The base-26 string representation.
        """
        if number == 0:
            return self.alphabet[0]

        result = []
        temp_num = number
        while temp_num > 0:
            result.append(self.alphabet[temp_num % self.base])
            temp_num //= self.base

        return "".join(reversed(result))

    def decode(self, b26_string: str) -> int:
        """Decode a base-26 string to an integer.

        Args:
            b26_string: The string to decode.

        Returns:
            The integer value.
        """
        number = 0
        for char in b26_string:
            number = number * self.base + self.alphabet.index(char)
        return number


@runtime_checkable
class ProcessingStage(Protocol):
    """Protocol for processing stages using duck typing."""

    title: str

    def process(self, data: Any) -> Any:
        """Process the given data.

        Args:
            data: The input data to process.

        Returns:
            The processed data.
        """
        ...


class InputStage:
    """Stage for input validation and parsing."""

    def __init__(self, title: str = "Cleaning and sorting fruits") -> None:
        self.title = title

    def process(
        self, data: Dict[str, Union[float, int, str]]
    ) -> Dict[str, Union[str, float]]:
        """Process input fruit data.

        Args:
            data: Raw fruit data dictionary.

        Returns:
            Cleaned data in kilograms.

        Raises:
            TypeError: If data is not a dictionary.
            KeyError: If required keys are missing.
            ValueError: If unit is unknown or fruits are rotten.
        """
        req_keys = ["fruit", "weight", "unit"]

        print(f"Input: {data}")

        if not isinstance(data, dict):
            raise TypeError("Data must be a dictionary.")

        missing_keys = [key for key in req_keys if key not in data]
        if missing_keys:
            raise KeyError(f"Missing keys: {missing_keys}")

        factors = {
            "mg": 0.001, "cg": 0.01, "dg": 0.1, "g": 1,
            "dag": 10, "hg": 100, "kg": 1000, "q": 100000, "t": 1000000
        }
        unit_key = str(data["unit"]).lower()

        if unit_key not in factors:
            raise ValueError(f"Unknown unit: {unit_key}")

        weight_in_grams = float(data["weight"]) * factors[unit_key]

        if random.randrange(1, 100) <= 30:
            rotten = round(time.time() % 100, 1) * factors[unit_key]
            if rotten >= weight_in_grams:
                raise ValueError("All fruits are rotten")

            data["weight"] = round(
                data["weight"] - rotten, 1
            )

            print(
                "The damaged fruit was discarded, "
                f"the remaining quantity is {data["weight"] / 1000}kg"
            )

        result: Dict[str, Union[float, str]] = {
            "fruit": str(data["fruit"]).capitalize(),
            "weight": weight_in_grams / 1000,
            "unit": "kg"
        }

        print(f"After {self.title}: {result}")

        return result


class TransformStage:
    """Stage for data transformation and enrichment."""

    def __init__(self, title: str = "Grinding and juicing the fruit") -> None:
        self.title = title

    def process(self, data: Dict[str, Any]) -> Dict[str, Union[str, float]]:
        """Implement transformation logic here."""
        if "weight" not in data:
            raise KeyError("Input error: weight missing in TransformStage")

        weight = float(data["weight"])
        if weight < 100:
            raise ValueError("Weight of fruits is to low (min 100kg)")
        elif weight > 1000000:
            raise ValueError("Weight of fruits is to high (max 1t)")

        result = {
            "unit": "L",
            "fruit": data["fruit"],
            "row weight": weight,
            "quantity": round(weight * random.uniform(0.75, 0.96), 1)
        }

        print(
            f"Transform: Transform fruit {result['fruit']} "
            f"to {result['fruit']} juice"
        )
        print(
            f"Result of transform: {result['fruit']} juice "
            f"({result['quantity']}L)"
        )

        return result


class OutputStage:
    """Stage for output formatting and delivery."""

    def __init__(self, title: str = "Juice canning and storage") -> None:
        self.title = title

    def process(self, data: Dict[str, Any]) -> Dict[str, Union[str, float]]:
        """Implement final output formatting here."""
        if "quantity" not in data:
            raise ValueError("Input error: quantity missing in OutputStage")

        extract = {
            "fruit": data["fruit"],
            "quantity": int((float(data["quantity"]) * 1000) / 80)
        }

        print(
            f"Output: Extract {extract['quantity']} bottles of "
            f"{extract['fruit']} juice from {data['row weight']} kg of "
            f"{data["fruit"]}"
        )

        return extract


class ProcessingPipeline(ABC):
    """Abstract base class for processing pipelines."""

    def __init__(self) -> None:
        self.stages: List[ProcessingStage] = []

    def add_stage(self, stage: ProcessingStage) -> None:
        """Add a processing stage."""
        self.stages.append(stage)

    @abstractmethod
    def process(self, data: Any) -> Any:
        """Abstract method for processing data."""
        pass


class JSONAdapter(ProcessingPipeline):
    """Adapter for JSON data format."""

    def __init__(self, pipeline_id: str) -> None:
        super().__init__()
        self.pipeline_id = pipeline_id

    def process(self, data: Dict[Any, Any]) -> Dict[str, Union[str, float]]:
        """Process JSON data through stages."""

        current = data
        for stage in self.stages:
            try:
                current = stage.process(current)
            except Exception as e:
                raise Exception(
                    f"Error detected in {stage.title}: {e}"
                )
        return current


class CSVAdapter(ProcessingPipeline):
    """Adapter for CSV data format."""

    def __init__(self, pipeline_id: str) -> None:
        super().__init__()
        self.pipeline_id = pipeline_id

    def process(self, data: str) -> Dict[str, Union[str, float]]:

        if isinstance(data, str):
            table = [
                line.split(',') for line in data.split('\n')
                if line.strip()
            ]
            if not table:
                raise ValueError("Empty CSV")
            raw_dict = {col[0]: list(col[1:]) for col in zip(*table)}
            current = {k: v[0] for k, v in raw_dict.items()}
        else:
            current = data

        for stage in self.stages:
            try:
                current = stage.process(current)
            except Exception as e:
                raise Exception(
                    f"Error detected in {stage.title}: {e}"
                )
        return current


class StreamAdapter(ProcessingPipeline):
    """Adapter for streaming data."""

    def __init__(self, pipeline_id: str) -> None:
        """Initialize with pipeline ID."""
        super().__init__()
        self.pipeline_id = pipeline_id

    def process(self, data: Any) -> Dict[str, Union[str, float]]:
        """Process stream data through stages."""

        current = data
        for stage in self.stages:
            try:
                current = stage.process(current)
            except Exception as e:
                raise Exception(
                    f"Error detected in {stage.title}: {e}"
                )
        return current


class NexusManager:
    """Manager to orchestrate multiple pipelines."""

    def __init__(self) -> None:
        self.pipelines: List[ProcessingPipeline] = []
        self.converter = Base26Converter()

        print("Initializing Nexus Manager...")
        print("Pipeline capacity: 20000 bottle/second\n")

    def add_pipeline(self, pipeline: ProcessingPipeline) -> None:
        """Add a pipeline to the manager."""
        self.pipelines.append(pipeline)

    def __find_my_name(self) -> str:
        """Search the caller's local scope for this instance's name."""
        try:
            frame = sys._getframe(2)
            for name, val in frame.f_locals.items():
                if val is self:
                    return name
        except (ValueError, AttributeError):
            pass
        return "Unknown"

    def chain_pipelines(self, data: Any) -> None:
        """Implement dynamic pipeline chaining logic using Base26 indexing."""
        error = False
        current = data
        chain: List[str] = []
        start_time = time.time()

        if not self.pipelines:
            raise ValueError(
                f"No Pipelines in NexusManager object {self.__find_my_name()}"
            )

        for i, pipe in enumerate(self.pipelines):
            p_id = f"Pipeline {self.converter.encode(i)}"
            try:
                adapter_type = pipe.__class__.__name__.removesuffix("Adapter")
                print(f"Processing {adapter_type} data through pipeline...")

                current = pipe.process(current)
                chain.append(p_id)
                print()
            except Exception as e:
                chain.append(f"{p_id} (STOP)")
                error = True
                print(f"Error in pipeline ({pipe.__class__.__name__}): {e}\n")
                break

        run_time = random.uniform(0.5, 1) + round(time.time() - start_time, 1)
        print("=== Pipelines Chaining Demo ===")

        if error:
            print(f"{' -> '.join(chain)}")
            completion = round(
                ((len(chain) - 1) / len(self.pipelines)) * 100, 1
            )
            print(f"Chain result: {completion}%")
            return

        print(" -> ".join(chain))
        print("Chain result: 100%")

        if isinstance(current, dict) and "quantity" in current:
            efficiency = (
                round((float(current["quantity"]) / run_time) / 200, 2)
            )
            print(
                f"Performance: {efficiency}% efficiency, "
                f"{run_time}s total processing time\n"
            )


def enterprise_pipeline() -> None:
    """Main function demonstrating dynamic chaining."""
    print("=== CODE NEXUS - ENTERPRISE PIPELINE SYSTEM ===\n")

    manager = NexusManager()

    pipes: Dict[ProcessingPipeline, List[ProcessingStage]] = {
        CSVAdapter("INPUT_ADAPTER"): [InputStage()],
        JSONAdapter("TRANSFORM_ADAPTER"): [TransformStage()],
        JSONAdapter("OUTPUT_ADAPTER"): [OutputStage()]
    }

    print("Creating Data Processing Pipelines...")
    for i, (pipe, stages) in enumerate(pipes.items(), 1):
        try:
            print(f"+ Pipeline {i}: {pipe.__class__.__name__}")
            for j, stage in enumerate(stages, 1):
                pipe.add_stage(stage)
                print(f"    - Stage {j}: {stage.title}")
            manager.add_pipeline(pipe)
            print()
        except Exception as e:
            raise Exception(
                f"Error in pipeline ({pipe}) in addition stage "
                f"({stage}): {e}\n"
            )

    try:
        print("=== Multi-Format Data Processing ===\n")
        manager.chain_pipelines("fruit,weight,unit\nGrapes,1200,kg")
    except Exception as e:
        print(f"Unexpected Error: {e}\n")

    try:
        print("=== Error Recovery Test ===\n")
        manager.chain_pipelines("fruit,weight,99\nGrapes,1200,kg")
    except Exception as e:
        print(f"Unexpected Error: {e}\n")

    print("\nNexus Integration complete. All systems operational.")


def main() -> None:
    try:
        enterprise_pipeline()
    except Exception as e:
        print(e, end="\n\n")


if __name__ == "__main__":
    main()
