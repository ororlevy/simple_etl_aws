import os
from typing import List
from transform.interfaces import FilesManager


class LocalFilesManager(FilesManager):
    def __init__(self, path: str):
        self.path = path

    def list_files(self) -> List[str]:
        files = []
        try:
            for f in os.listdir(self.path):
                f_path = os.path.join(self.path, f)
                if os.path.isfile(f_path):
                    files.append(f_path)
            return files
        except OSError as e:
            raise Exception("could not list files {}".format(e))

    def download_file(self, file_name: str) -> bytes:
        try:
            with open(os.path.join(self.path, file_name), 'rb') as file:
                return file.read()
        except OSError as e:
            raise Exception("could not download file {}".format(e))

    def upload_file(self, file_name: str, data: bytes) -> None:
        try:
            with open(os.path.join(self.path, file_name), 'wb') as file:
                file.write(data)
        except OSError as e:
            raise Exception("could not upload file {}".format(e))
