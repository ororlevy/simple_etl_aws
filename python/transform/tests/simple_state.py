from abc import ABC
from typing import Optional

from transform.interfaces import StateManager, FilesManager


class SimpleState(StateManager[str], ABC):

    def __init__(self, state_file_name, files_manager: FilesManager):
        self.files_manager = files_manager
        self.state_file_name = state_file_name

    def get_state(self) -> Optional[str]:
        try:
            state = self.files_manager.download_file(self.state_file_name)
            return state
        except Exception as _:
            return None

    def update_state(self, state):
        self.files_manager.upload_file(self.state_file_name, state.encode('utf-8'))
