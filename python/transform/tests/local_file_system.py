import os
from typing import List
from transform.interfaces import FilesManager


class LocalFilesManager(FilesManager):
    def get_files(self, path: str) -> List[str]:
        files = []
        try:
            for f in os.listdir(path):
                f_path = os.path.join(path, f)
                if os.path.isfile(f_path):
                    files.append(f_path)
            return files
        except OSError as e:
            raise Exception("could not list files {}".format(e))

    def download_file(self, file_path: str) -> str:
        try:
            with open(file_path, 'r') as file:
                return file.read()
        except OSError as e:
            raise Exception("could not download file {}".format(e))

    def upload_file(self, file_path: str, data: bytes) -> None:
        try:
            with open(file_path, 'wb') as file:
                file.write(data)
        except OSError as e:
            raise Exception("could not upload file {}".format(e))
