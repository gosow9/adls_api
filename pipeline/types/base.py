from abc import ABC, abstractmethod
from typing import Dict, Any

class BasePipelineType(ABC):
    """
    Abstract base class for different pipeline types. 
    Each pipeline type can define its own tasks (Task classes).
    """

    def __init__(self, pipeline_config: Dict[str, Any]):
        self.pipeline_config = pipeline_config

    @abstractmethod
    def get_tasks(self) -> Dict[str, Any]:
        """
        Return a dictionary of {task_name: task_instance}.
        Subclasses should implement how tasks are defined for that pipeline type.
        """
        pass
