import yaml
import os
from typing import Dict, Any, List, Union

from pipeline.types.base import BasePipelineType
from pipeline.types.replicator import ReplicatorPipelineType
# Import any other custom pipeline type classes as needed

# Simple registry of pipeline types
PIPELINE_TYPE_REGISTRY = {
    "replicator": ReplicatorPipelineType,
    # "custom_type": CustomPipelineType,   # Example placeholder for new pipeline types
}

class Pipeline:
    """
    The Pipeline class is responsible for:
    1. Reading pipeline config from YAML.
    2. Instantiating the appropriate pipeline type (set of tasks).
    3. Creating a metadata object (or engine) used by tasks.
    4. Running tasks in requested order.
    """

    def __init__(self, root_path: str, yaml_name: str, pipeline_name: str):
        """
        :param root_path: Folder path where YAML is located
        :param yaml_name: Filename of the YAML
        :param pipeline_name: The specific pipeline's name in the YAML to load
        """
        yaml_path = os.path.join(root_path, yaml_name)
        self.pipeline_config = self._load_config(yaml_path, pipeline_name)
        
        # Build pipeline type from registry
        pipeline_type_str = self.pipeline_config.get("type")
        pipeline_type_cls = PIPELINE_TYPE_REGISTRY.get(pipeline_type_str)
        if not pipeline_type_cls:
            raise ValueError(f"Pipeline type '{pipeline_type_str}' not found in registry.")
        
        # Instantiate pipeline type
        self.pipeline_type: BasePipelineType = pipeline_type_cls(self.pipeline_config)
        
        # Generate metadata once for the entire pipeline
        self.metadata = self._create_metadata(self.pipeline_config)
        
        # The pipeline type can define tasks in a dictionary {task_name: TaskClass or TaskInstance, ...}
        self.tasks = self.pipeline_type.get_tasks()

    def _load_config(self, yaml_path: str, pipeline_name: str) -> Dict[str, Any]:
        """
        Reads the YAML file, finds the pipeline config that matches pipeline_name
        """
        with open(yaml_path, 'r') as f:
            all_configs = yaml.safe_load(f)

        for config in all_configs:
            if config.get("pipeline_name") == pipeline_name:
                return config
        raise ValueError(f"Pipeline with name '{pipeline_name}' not found in {yaml_path}")

    def _create_metadata(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Creates a metadata object that tasks can use. 
        This is a simple dict, but you can make a more complex class if needed.
        """
        # For example: 
        metadata = {
            "pipeline_name": config.get("pipeline_name"),
            "sql_path": config.get("sql_path"),
            "dataset_type": config.get("dataset_type"),
            "partition_key": config.get("partition_key"),
            "target_format": config.get("target_format"),
            # ... add more fields as needed ...
        }
        return metadata

    def run_task(self, task_names: Union[str, List[str]]):
        """
        Runs one or multiple tasks, passing the shared metadata.
        Example usage:
            pipeline.run_task("task1")
            pipeline.run_task("task1,task2,task3")
        """
        if isinstance(task_names, str):
            # If comma-separated, split them
            task_names = [t.strip() for t in task_names.split(",")]
        
        for name in task_names:
            task = self.tasks.get(name)
            if not task:
                raise ValueError(f"Task '{name}' does not exist in pipeline '{self.pipeline_type.pipeline_config['pipeline_name']}'.")
            
            print(f"\n--- Running Task: {name} ---")
            task.run(self.metadata)
            print(f"--- Finished Task: {name} ---\n")
