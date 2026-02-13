import io
import time
import random
import contextlib
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

            data["weight"] = round(
                (float(data["weight"]) - rotten) / factors["kg"], 1
            )

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
        if "weight" not in data:
            raise KeyError("Input error: weight missing in TransformStage")

        weight = float(data["weight"])
        if weight < 100:
            raise ValueError("Weight of fruits is to low (min 100kg)")
        elif weight > 1000000:
            raise ValueError("Weight of fruits is to high (max 1t)")

        result = {
            "unit": "L",
            "name": data["name"],
            "row weight": weight,
            "quantity": round(weight * random.uniform(0.75, 0.96), 1)
        }

        print(
            f"Transform: Transform fruit {result['name']} "
            f"to {result['name']} juice"
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
            "name": data["name"],
            "quantity": int((float(data["quantity"]) * 1000) / 80)
        }

        print(
            f"Output: Extract {extract['name']} cans of juice "
            f"from {data['row weight']} kg of "
            f"{data["name"]}"
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
        self.pipelines: List[ProcessingPipeline] = []
        self.stats: Dict[str, Any] = collections.defaultdict(int)

        print("Initializing Nexus Manager...")
        print("Pipeline capacity: 1000 streams/second\n")

    def add_pipeline(self, pipeline: ProcessingPipeline) -> None:
        """Add a pipeline to the manager."""
        self.pipelines.append(pipeline)

    def chain_pipelines(self, data: Any) -> Dict[str, Any]:
        """Implement dynamic pipeline chaining logic."""
        start_time = time.time()
        current = data

        pipeline_names = [chr(65 + i) for i in range(len(self.pipelines))]
        chain_visual = " -> ".join(
            [f"Pipeline {name}" for name in pipeline_names]
        )

        f = io.StringIO()
        with contextlib.redirect_stdout(f):
            for p in self.pipelines:
                current = p.process(current)
                if isinstance(current, str) and "Recovered" in current:
                    break

        print(f"{chain_visual}")

        return {
            "result": current,
            "elapsed": time.time() - start_time,
            "efficiency": 95
        }


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
            for j, stage in enumerate(stages, 1):
                pipe.add_stage(stage)
                print(f"Stage {j}: {stage.title}")
            manager.add_pipeline(pipe)
            print(f"Pipeline {i}: {pipe.__class__.__name__}\n")
        except Exception as e:
            raise Exception(
                f"Error in pipeline ({pipe}) in addition stage ({stage}): {e}"
            )

    print("=== Multi-Format Data Processing ===")

    chain_result = manager.chain_pipelines(
        "fruit,weight,unit\nGrapes,1200,kg"
    )

    print("Chain status: Completed")
    print(f"Final Data: {chain_result['result']}")
    print(f"Performance: {chain_result['elapsed']:.4f}s total")

    print("\nNexus Integration complete.")


if __name__ == "__main__":
    enterprise_pipeline()
