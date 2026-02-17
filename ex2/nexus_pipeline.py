import time
import random
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Protocol, Union, runtime_checkable


GRAM_TO_KG = 1000
L_TO_ML = 1000
BOTTLE_VOLUME_ML = 80
MIN_JUICE_YIELD = 0.75
MAX_JUICE_YIELD = 0.96
MIN_WEIGHT_KG = 100
MAX_WEIGHT_KG = 1000000
ROTTEN_CHANCE = 30
PIPELINE_CAPACITY = 20000


class Base26Converter:
    """A converter for base-10 to base-26 (alphabetical) and vice versa.

    Used for generating spreadsheet-like column identifiers (A, B, C... Z,
    AA).
    """

    def __init__(self) -> None:
        """Initializes the converter with uppercase English alphabet."""
        self.alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        self.base = len(self.alphabet)

    def encode(self, number: int) -> str:
        """Encode an integer to a base-26 string.

        Args:
            number: The integer to encode.

        Returns:
            str: The base-26 string representation.
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
            int: The integer value.
        """
        number = 0
        for char in b26_string:
            number = number * self.base + self.alphabet.index(char)
        return number


@runtime_checkable
class ProcessingStage(Protocol):
    """Protocol for processing stages using duck typing.

    Any class that provides a process() method qualifies as a stage.
    """

    title: str

    def process(self, data: Any) -> Any:
        """Process the given data.

        Args:
            data: The input data to process.

        Returns:
            Any: The processed data.
        """
        ...


class InputStage:
    """Stage for input validation and parsing.

    Handles fruit cleaning, sorting, and unit conversion to kilograms.
    """

    def __init__(self, title: str = "Cleaning and sorting fruits") -> None:
        """Initializes the input stage with a default or custom title.

        Args:
            title: The descriptive title of the stage.
        """
        self.title = title

    def process(self, data: Any) -> Dict[str, Union[str, float]]:
        """Process input fruit data and handle weight reduction for rotten
        items.

        Args:
            data: Raw fruit data dictionary containing 'fruit', 'weight',
                  and 'unit'.

        Returns:
            Dict[str, Union[str, float]]: Cleaned data standardized to kg.

        Raises:
            TypeError: If data is not a dictionary.
            KeyError: If required keys are missing.
            ValueError: If unit is unknown, weight is invalid, or all
                        fruits are rotten.
        """
        req_keys = ["fruit", "weight", "unit"]
        print(f"Input: {data}")

        if not isinstance(data, dict):
            raise TypeError("Data must be a dictionary.")

        missing = [k for k in req_keys if k not in data]
        if missing:
            raise KeyError(f"Missing keys: {missing}")

        factors: Dict[str, float] = {
            "mg": 0.001, "cg": 0.01, "dg": 0.1, "g": 1,
            "dag": 10, "hg": 100, "kg": GRAM_TO_KG,
            "q": 100000, "t": 1000000
        }
        unit_key = str(data["unit"]).lower()
        if unit_key not in factors:
            raise ValueError(f"Unknown unit: {unit_key}")

        try:
            weight_val = float(data["weight"])
        except (TypeError, ValueError):
            raise ValueError(f"Invalid weight value: {data['weight']}")

        weight_in_grams = weight_val * factors[unit_key]

        if random.randrange(1, 100) <= ROTTEN_CHANCE:
            rotten_g = round(time.time() % 100, 1) * factors[unit_key]
            if rotten_g >= weight_in_grams:
                raise ValueError("All fruits are rotten")
            weight_in_grams -= rotten_g
            print(
                "The damaged fruit was discarded, "
                f"the remaining quantity is "
                f"{round(weight_in_grams / GRAM_TO_KG, 3)}kg"
            )

        result: Dict[str, Union[float, str]] = {
            "fruit": str(data["fruit"]).capitalize(),
            "weight": weight_in_grams / GRAM_TO_KG,
            "unit": "kg"
        }
        print(f"After {self.title}: {result}")
        return result


class TransformStage:
    """Stage for data transformation and enrichment.

    Simulates the grinding and juicing process with random yield efficiency.
    """

    def __init__(
        self, title: str = "Grinding and juicing the fruit"
    ) -> None:
        """Initializes the transformation stage.

        Args:
            title: Descriptive title of the transformation stage.
        """
        self.title = title

    def process(self, data: Any) -> Dict[str, Union[str, float]]:
        """Transforms fruit weight into juice volume (Liters).

        Args:
            data: Dictionary containing fruit type and weight in kg.

        Returns:
            Dict[str, Union[str, float]]: Results of the juicing process.

        Raises:
            KeyError: If weight is missing.
            ValueError: If weight is outside allowed factory bounds.
        """
        if not isinstance(data, dict) or "weight" not in data:
            raise KeyError("Input error: weight missing in TransformStage")

        weight = float(data["weight"])
        if weight < MIN_WEIGHT_KG:
            raise ValueError(
                f"Weight of fruits is too low (min {MIN_WEIGHT_KG}kg)"
            )
        if weight > MAX_WEIGHT_KG:
            raise ValueError("Weight of fruits is too high (max 1t)")

        result: Dict[str, Union[str, float]] = {
            "unit": "L",
            "fruit": data["fruit"],
            "row weight": weight,
            "quantity": round(
                weight * random.uniform(MIN_JUICE_YIELD, MAX_JUICE_YIELD),
                1
            )
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
    """Stage for output formatting and delivery.

    Handles the final packaging of juice into standardized bottles.
    """

    def __init__(self, title: str = "Juice canning and storage") -> None:
        """Initializes the output stage.

        Args:
            title: Descriptive title of the output stage.
        """
        self.title = title

    def process(self, data: Any) -> Dict[str, Union[str, float]]:
        """Calculates the number of bottles extracted from juice volume.

        Args:
            data: Dictionary containing quantity in Liters and raw weight.

        Returns:
            Dict[str, Union[str, float]]: Final extraction statistics.

        Raises:
            ValueError: If quantity is missing from the data.
        """
        if not isinstance(data, dict) or "quantity" not in data:
            raise ValueError(
                "Input error: quantity missing in OutputStage"
            )

        extract: Dict[str, Union[str, float]] = {
            "fruit": data["fruit"],
            "quantity": int(
                (float(data["quantity"]) * L_TO_ML) / BOTTLE_VOLUME_ML
            )
        }
        print(
            f"Output: Extract {extract['quantity']} bottles of "
            f"{extract['fruit']} juice from {data['row weight']} kg of "
            f"{data['fruit']}"
        )
        return extract


class ProcessingPipeline(ABC):
    """Abstract base class for processing pipelines.

    Defines the structure for adding stages and executing them.
    """

    def __init__(self) -> None:
        """Initializes the pipeline with an empty list of stages."""
        self.__stages: List[ProcessingStage] = []
        self.pipeline_id: str = "Pipeline"

    def add_stage(self, stage: ProcessingStage) -> None:
        """Registers a processing stage to the pipeline.

        Args:
            stage: An object that satisfies the ProcessingStage protocol.
        """
        if isinstance(stage, ProcessingStage):
            self.__stages.append(stage)

    def get_stages(self) -> List[ProcessingStage]:
        return self.__stages

    @abstractmethod
    def process(self, data: Any) -> Any:
        """Abstract method to define data flow through the pipeline.

        Args:
            data: Input data to be processed.
        """
        pass


class JSONAdapter(ProcessingPipeline):
    """Adapter for processing data in JSON/Dictionary format."""

    def __init__(self, pipeline_id: str) -> None:
        """Initializes JSON adapter with a specific ID.

        Args:
            pipeline_id: Unique identifier for the pipeline.
        """
        super().__init__()
        self.pipeline_id = pipeline_id

    def process(self, data: Any) -> Any:
        """Sequentially executes stages on JSON data.

        Args:
            data: Dictionary data input.

        Returns:
            Any: Final processed result.

        Raises:
            Exception: If any stage execution fails.
        """
        current = data
        stages = self.get_stages()

        for stage in stages:
            try:
                current = stage.process(current)
            except (TypeError, KeyError, ValueError) as e:
                raise Exception(
                    f"Error detected in {stage.title}: {e}"
                )
        return current


class CSVAdapter(ProcessingPipeline):
    """Adapter for processing data provided in CSV string format."""

    def __init__(self, pipeline_id: str) -> None:
        """Initializes CSV adapter with a specific ID.

        Args:
            pipeline_id: Unique identifier for the pipeline.
        """
        super().__init__()
        self.pipeline_id = pipeline_id

    def process(self, data: Any) -> Any:
        """Parses CSV string and passes it through processing stages.

        Accepts either a CSV-formatted string or a pre-parsed dictionary,
        maintaining interface compatibility with the base class signature.

        Args:
            data: CSV formatted string or dictionary.

        Returns:
            Any: Processed data dictionary.

        Raises:
            ValueError: If CSV data is empty.
            Exception: If any stage execution fails.
        """
        stages = self.get_stages()

        if isinstance(data, str):
            table = [
                line.split(',') for line in data.split('\n')
                if line.strip()
            ]
            if not table:
                raise ValueError("Empty CSV")
            raw_dict = {col[0]: list(col[1:]) for col in zip(*table)}
            current: Any = {k: v[0] for k, v in raw_dict.items()}
        else:
            current = data

        for stage in stages:
            try:
                current = stage.process(current)
            except (TypeError, KeyError, ValueError) as e:
                raise Exception(
                    f"Error detected in {stage.title}: {e}"
                )
        return current


class StreamAdapter(ProcessingPipeline):
    """Adapter for real-time streaming data processing."""

    def __init__(self, pipeline_id: str) -> None:
        """Initializes Stream adapter.

        Args:
            pipeline_id: Unique identifier for the stream.
        """
        super().__init__()
        self.pipeline_id = pipeline_id

    def process(self, data: Any) -> Union[Any, List[Any]]:
        """Passes streaming data packets through pipeline stages.

        Args:
            data: Stream data packet.

        Returns:
            Any: Processed result.

        Raises:
            Exception: If any stage execution fails.
        """
        stages = self.get_stages()
        current: List[Any] = data if isinstance(data, list) else [data]

        for i, _ in enumerate(current):
            for stage in stages:
                try:
                    current[i] = stage.process(current[i])
                except (TypeError, KeyError, ValueError) as e:
                    raise Exception(
                        f"Error detected in {stage.title}: {e}"
                    )
        return current if isinstance(data, list) else current[0]


class NexusManager:
    """Manager to orchestrate and chain multiple pipelines.

    Handles high-level coordination, error recovery, and performance metrics.
    """

    def __init__(self, name: str = "NexusManager") -> None:
        """Initializes Nexus Manager and its converters.

        Args:
            name: Human-readable identifier for this manager instance.
        """
        self.name = name
        self.__converter = Base26Converter()
        self.__pipelines: List[ProcessingPipeline] = []
        self.__backup_pipelines: List[ProcessingPipeline] = []

        print("Initializing Nexus Manager...")
        print(f"Pipeline capacity: {PIPELINE_CAPACITY} bottle/second\n")

    def add_pipeline(self, pipeline: ProcessingPipeline) -> None:
        """Adds a pipeline instance to the orchestration list.

        Args:
            pipeline: The ProcessingPipeline object to add.
        """
        if isinstance(pipeline, ProcessingPipeline):
            self.__pipelines.append(pipeline)

    def add_backup_pipeline(self, pipeline: ProcessingPipeline) -> None:
        """Registers a backup pipeline used during error recovery.

        Args:
            pipeline: The ProcessingPipeline object to use as backup.
        """
        if isinstance(pipeline, ProcessingPipeline):
            self.__backup_pipelines.append(pipeline)

    def __try_all_backups(self, data: Any) -> Any:
        """Tries every registered backup pipeline in order until one succeeds.

        Args:
            data: The data to pass to each backup pipeline.

        Returns:
            Any: Result produced by the first successful backup pipeline.

        Raises:
            Exception: If no backups are registered or all of them fail.
        """
        if not self.__backup_pipelines:
            raise Exception("No backup pipelines available for recovery.")

        last_err: Exception = Exception("All backup pipelines failed.")
        for backup in self.__backup_pipelines:
            try:
                print(
                    "Recovery initiated: Switching to backup processor "
                    f"({backup.__class__.__name__} / "
                    f"{backup.pipeline_id})"
                )
                result = backup.process(data)
                print(
                    "Recovery successful: Pipeline restored, "
                    "processing resumed"
                )
                return result
            except Exception as err:
                print(f"  Backup {backup.pipeline_id} failed: {err}\n")
                last_err = err

        raise last_err

    def chain_pipelines(self, data: Any) -> None:
        """Chains all registered pipelines to process data sequentially.

        On failure, tries every registered backup pipeline in order before
        marking the chain as stopped.

        Args:
            data: The initial data input for the chain.

        Raises:
            ValueError: If no pipelines are registered in the manager.
        """
        if not self.__pipelines:
            raise ValueError(
                f"No Pipelines in NexusManager object {self.name}"
            )

        error = False
        current = data
        chain: List[str] = []
        start_time = time.time()

        for i, pipe in enumerate(self.__pipelines):
            p_id = f"Pipeline {self.__converter.encode(i)}"
            adapter_type = pipe.__class__.__name__.removesuffix("Adapter")
            print(f"Processing {adapter_type} data through pipeline...")

            try:
                current = pipe.process(current)
                chain.append(p_id)
                print()
            except Exception as e:
                print(
                    f"Error in pipeline ({pipe.__class__.__name__}): {e}\n"
                )
                try:
                    current = self.__try_all_backups(current)
                    chain.append(f"{p_id} (recovered)")
                    print()
                except Exception as recovery_err:
                    chain.append(f"{p_id} (STOP)")
                    error = True
                    print(f"Recovery failed: {recovery_err}\n")
                    break

        run_time = (
            random.uniform(0.5, 1) + round(time.time() - start_time, 1)
        )
        print("=== Pipelines Chaining Demo ===")

        if error:
            print(f"{' -> '.join(chain)}")
            completion = round(
                ((len(chain) - 1) / len(self.__pipelines)) * 100, 1
            )
            print(f"Chain result: {completion}%")
            return

        print(" -> ".join(chain))
        print("Chain result: 100%")

        if isinstance(current, dict) and "quantity" in current:
            efficiency = round(
                (float(current["quantity"]) / run_time) / 200, 2
            )
            print(
                f"Performance: {efficiency}% efficiency, "
                f"{run_time:.1f}s total processing time\n"
            )


def enterprise_pipeline() -> None:
    """Sets up and executes an enterprise-grade processing chain.

    Coordinates CSV and JSON adapters with multiple processing stages
    and demonstrates error recovery via backup pipelines.
    """
    print("=== CODE NEXUS - ENTERPRISE PIPELINE SYSTEM ===\n")

    manager = NexusManager(name="manager")

    pipes: Dict[ProcessingPipeline, List[ProcessingStage]] = {
        StreamAdapter("INPUT_ADAPTER"): [InputStage()],
        JSONAdapter("TRANSFORM_ADAPTER"): [TransformStage()],
        JSONAdapter("OUTPUT_ADAPTER"): [OutputStage()]
    }

    backup1 = JSONAdapter("JSON_BACKUP_ADAPTER")
    backup1.add_stage(InputStage(title="JSON Backup validation"))

    backup2 = CSVAdapter("CSV_BACKUP_ADAPTER")
    backup2.add_stage(InputStage(title="CSV Backup validation"))

    print("Creating Data Processing Pipelines...")
    for i, (pipe, stages) in enumerate(pipes.items(), 1):
        try:
            print(f"+ Pipeline {i}: {pipe.__class__.__name__}")
            for j, stage in enumerate(stages, 1):
                pipe.add_stage(stage)
                print(f"  - Stage {j}: {stage.title}")
            manager.add_pipeline(pipe)
            print()
        except Exception as e:
            raise Exception(
                f"Error in pipeline ({pipe}) in addition stage "
                f"({stage}): {e}\n"
            )

    manager.add_backup_pipeline(backup1)
    manager.add_backup_pipeline(backup2)

    try:
        print("=== Multi-Format Data Processing ===\n")
        manager.chain_pipelines(
            {'fruit': 'Grapes', 'weight': '1200', 'unit': 'kg'}
        )
    except Exception as e:
        print(f"Unexpected Error: {e}\n")

    try:
        print("=== Error Recovery Test ===\n")
        manager.chain_pipelines("fruit,weight,unit\nGrapes,1200,kg")
    except Exception as e:
        print(f"Unexpected Error: {e}\n")

    print("\nNexus Integration complete. All systems operational.")


def main() -> None:
    """Main execution entry point."""
    try:
        enterprise_pipeline()
    except Exception as e:
        print(f"Unexpected Error: {e}\n")


if __name__ == "__main__":
    main()
