from pipeline.tasks.base import BaseTask
from typing import Dict, Any

class ReplicatorExtractTask(BaseTask):
    def run(self, metadata: Dict[str, Any]) -> None:
        print("Extracting data...")
        # Example usage of metadata
        print(f"  SQL Path: {metadata.get('sql_path')}")
        print(f"  Dataset Type: {metadata.get('dataset_type')}")
        # ... do real extraction logic here ...
        print("Extraction complete.")

class ReplicatorTransformTask(BaseTask):
    def run(self, metadata: Dict[str, Any]) -> None:
        print("Transforming data...")
        print(f"  Using partition key: {metadata.get('partition_key')}")
        # ... transform logic ...
        print("Transformation complete.")

class ReplicatorLoadTask(BaseTask):
    def run(self, metadata: Dict[str, Any]) -> None:
        print("Loading data...")
        print(f"  Target format: {metadata.get('target_format')}")
        # ... load logic ...
        print("Loading complete.")
