from abc import ABC, abstractmethod
from typing import List

import pandas as pd


class FilesManager(ABC):
    @abstractmethod
    def list_files(self) -> List[str]:
        """Fetch the list of file paths from the given directory or bucket."""
        pass

    @abstractmethod
    def download_file(self, file_name: str) -> bytes:
        """Download a file and return its content as a string."""
        pass

    @abstractmethod
    def upload_file(self, file_name: str, data: bytes) -> None:
        """Upload the given data to the specified file path."""
        pass


class Mapper(ABC):
    NAME_ATTRIBUTE = 'name'

    def __init__(self):
        self.df = None

    @abstractmethod
    def transform(self, df: pd.DataFrame) -> List[pd.DataFrame]:
        """Transform the given dataframe to multiple frames to support additional outputs, for example group by.

        The transform method should also add a 'name' attribute to each resulting DataFrame
        as an attribute (not a column), to ensure they can be identified and used appropriately
        in subsequent processes.

        """
        pass


class StateManager(ABC):
    @abstractmethod
    def get_state(self) -> str:
        """Get the current state, return None if no state is available."""
        pass

    @abstractmethod
    def update_state(self, state: str) -> None:
        """Update the state."""
        pass
