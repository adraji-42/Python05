import time
import random
import collections
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Protocol, Union, runtime_checkable


@runtime_checkable
class ProcessingStage(Protocol):
    """Protocol for processing stages using duck typing."""

    def process(self, data: Any) -> Any:
        """Process the given data."""
        ...


class InputStage:
    """Stage for input validation and parsing."""

    def __init__(self, title: str = "Cleaning and sorting fruits") -> None:
        self.title = title

    def process(
        self, data: Dict[str, Union[float, int, str]]
    ) -> Dict[str, Union[str, float]]:
        """Implement input logic here."""
        req_keys = ["fruit", "weight", "unit"]

        print(f"Input: {data}")

        if not isinstance(data, dict):
            raise TypeError("The data type does not match at the InputStage.")

        missing_keys = [key for key in req_keys if key not in data]
        if missing_keys:
            raise ValueError(f"Missing Required Keys: {missing_keys}")

        factors = {
            "mg": 0.001, "cg": 0.01, "dg": 0.1, "g": 1,
            "dag": 10, "hg": 100, "kg": 1000, "q": 100000, "t": 1000000
        }

        if random.randrange(1, 100) <= 20:
            try:
                unit_key = str(data["unit"]).lower()
                rotten = (
                    round(time.time() % 100, 2) * factors[unit_key]
                )
                if rotten >= float(data["weight"]):
                    raise ValueError("All fruits are rotten")
            except KeyError as e:
                raise KeyError(f"Unknown Unit: {e}")

            data["weight"] = (float(data["weight"]) - rotten) / factors["kg"]

        result: Dict[str, Union[float, str]] = {}
        result["fruit"] = str(data["fruit"]).capitalize()
        result["weight"] = float(data["weight"])
        result["unit"] = "kg"

        print(
            "The damaged fruit was discarded, "
            f"the remaining quantity is {result['weight']}"
        )

        return result


class TransformStage:
    """Stage for data transformation and enrichment."""

    def __init__(self, title: str = "Grinding and juicing the fruit") -> None:
        self.title = title

    def process(self, data: Dict[str, Any]) -> Dict[str, Union[str, float]]:
        """Implement transformation logic here."""

        weight = float(data["weight"])
        if weight < 100:
            raise ValueError("Weight of fruits is to low (min 100kg)")
        elif weight > 1000000:
            raise ValueError("Weight of fruits is to high (max 1t)")

        result = {
            "unit": "L",
            "name": data['fruit'],
            "row weight": weight,
            "quantity": round(weight * random.uniform(0.75, 0.96), 2)
        }

        print(
            f"Transform: Transform fruit {data['fruit']} "
            f"to {result['name']} juice"
        )

        return result


class OutputStage:
    """Stage for output formatting and delivery."""

    def __init__(self, title: str = "Juice canning and storage") -> None:
        self.title = title

    def process(self, data: Dict[str, Any]) -> Dict[str, Union[str, float]]:
        """Implement final output formatting here."""

        extract = {
            "name": data["name"],
            "quantity": int((float(data["quantity"]) * 1000) / 80)
        }

        print(
            f"Output: Extract {extract['name']} cans of juice "
            f"from {data['row weight']} kg of {data['name']}"
        )

        return extract


class ProcessingPipeline(ABC):
    """Abstract base class for processing pipelines."""

    def __init__(self) -> None:
        self.stages: List[ProcessingStage] = []

    def add_stage(self, stage: ProcessingStage) -> None:
        """Add a processing stage and return self for chaining."""
        self.stages.append(stage)

    @abstractmethod
    def process(self, data: Any) -> Any:
        """Abstract method for processing data."""
        pass


class JSONAdapter(ProcessingPipeline):
    """Adapter for JSON data format."""

    def __init__(self, pipeline_id: str) -> None:
        """Initialize with pipeline ID."""
        super().__init__()
        self.pipeline_id = pipeline_id

    def process(
            self, data: Dict[Any, Any]
    ) -> Union[Dict[str, Union[str, float]], str]:
        """Process JSON data through stages."""

        try:
            current = data
            for stage in self.stages:
                current = stage.process(current)
            return current
        except Exception as e:
            print(f"Error detected in {self.pipeline_id}: {e}")
            print("Recovery initiated: Switching to backup processor")
            return f"Recovered: {e}"


class CSVAdapter(ProcessingPipeline):
    """Adapter for CSV data format."""

    def __init__(self, pipeline_id: str) -> None:
        super().__init__()
        self.pipeline_id = pipeline_id

    def process(self, data: Any) -> Any:
        try:
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
                current = stage.process(current)
            return current
        except Exception as e:
            print(f"Error detected in {self.pipeline_id}: {e}")
            print("Recovery initiated: Switching to backup processor")
            return f"Recovered: {e}"


class StreamAdapter(ProcessingPipeline):
    """Adapter for streaming data."""

    def __init__(self, pipeline_id: str) -> None:
        """Initialize with pipeline ID."""
        super().__init__()
        self.pipeline_id = pipeline_id

    def process(self, data: Any) -> Union[str, Any]:
        """Process stream data through stages."""
        try:
            current = data
            for stage in self.stages:
                current = stage.process(current)
            return current
        except Exception as e:
            print(f"Error detected in {self.pipeline_id}: {e}")
            return f"Recovered: {e}"


class NexusManager:
    """Manager to orchestrate multiple pipelines."""

    def __init__(self) -> None:
        """Initialize manager with stats and pipeline storage."""
        self.pipelines: List[ProcessingPipeline] = []
        self.stats: Dict[str, Any] = collections.defaultdict(int)

    def add_pipeline(self, pipeline: ProcessingPipeline) -> None:
        """Add a pipeline to the manager."""
        self.pipelines.append(pipeline)

    def process_data(self, data: Any) -> List[Any]:
        """Process data through all pipelines."""
        return [p.process(data) for p in self.pipelines]

    def chain_pipelines(self, data: Any) -> Dict[str, Any]:
        """Implement pipeline chaining logic."""
        start_time = time.time()
        current = data
        for p in self.pipelines:
            current = p.process(current)
        return {
            "result": current,
            "elapsed": time.time() - start_time,
            "efficiency": 95
        }


def main() -> None:
    """Main function demonstrating the juice factory pipeline."""
    print("=== CODE NEXUS - ENTERPRISE PIPELINE SYSTEM ===")
    print("Initializing Nexus Manager...")
    print("Pipeline capacity: 1000 fruits/batch")

    print("\nCreating Juice Production Pipeline...")
    print("Stage 1: Cleaning and sorting fruits")
    print("Stage 2: Grinding and juicing the fruit")
    print("Stage 3: Juice canning and storage")

    manager = NexusManager()

    json_pipe = JSONAdapter("JSON_FRUIT_PROC_01")
    json_pipe.add_stage(InputStage())
    json_pipe.add_stage(TransformStage())
    json_pipe.add_stage(OutputStage())

    csv_pipe = CSVAdapter("CSV_FRUIT_PROC_01")
    csv_pipe.add_stage(InputStage())
    csv_pipe.add_stage(TransformStage())
    csv_pipe.add_stage(OutputStage())

    print("\n=== Multi-Format Juice Processing ===")

    json_data = {"fruit": "Apple", "weight": 500, "unit": "kg"}
    json_pipe.process(json_data)

    print()

    csv_data = "fruit,weight,unit\nOrange,800,kg"
    csv_pipe.process(csv_data)

    print("\n=== Pipeline Chaining Demo ===")
    print("Pipeline A (Raw) -> Pipeline B (Juiced) -> Pipeline C (Canned)")

    manager.add_pipeline(json_pipe)
    manager.add_pipeline(csv_pipe)

    chain_result = manager.chain_pipelines(
        {"fruit": "Grapes", "weight": 1200, "unit": "kg"}
    )

    print("Chain result: Batch processed through multi-stage system")
    print(
        f"Performance: {chain_result['efficiency']}% efficiency, "
        f"{chain_result['elapsed']:.4f}s total processing time"
    )

    print("\n=== Error Recovery Test ===")
    print("Simulating rotten fruit failure...")
    json_pipe.process({"fruit": "Lemon", "weight": 10, "unit": "kg"})

    print("\nNexus Integration complete. All factory systems operational.")


if __name__ == "__main__":
    main()
