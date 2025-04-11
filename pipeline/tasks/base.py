from abc import ABC, abstractmethod
from typing import Dict, Any

class BaseTask(ABC):
    @abstractmethod
    def run(self, metadata: Dict[str, Any]) -> None:
        """
        Execute the logic for this task. 
        `metadata` is a dict that contains everything needed to run tasks.
        """
        pass
