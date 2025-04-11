from typing import Dict, Any
from pipeline.types.base import BasePipelineType
from pipeline.tasks.example import (
    ReplicatorExtractTask,
    ReplicatorTransformTask,
    ReplicatorLoadTask
)

class ReplicatorPipelineType(BasePipelineType):
    """
    Example pipeline type, called "replicator".
    Typically: Extract -> Transform -> Load (ETL).
    """

    def get_tasks(self) -> Dict[str, Any]:
        """
        Defines tasks for a 'replicator' pipeline.
        Note that we create instances of these tasks here, 
        or you could store references to the class and instantiate later.
        """
        return {
            "task1": ReplicatorExtractTask(),
            "task2": ReplicatorTransformTask(),
            "task3": ReplicatorLoadTask(),
            # You can define more tasks if needed
        }
