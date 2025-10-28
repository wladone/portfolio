import logging
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


class FileTailReader:
    def __init__(self, file_path: str, batch_size: int = 100):
        self.file_path = Path(file_path)
        self.batch_size = batch_size
        self.last_position = 0
        self.file_handle = None

    def open_file(self):
        if not self.file_path.exists():
            raise FileNotFoundError(f"File {self.file_path} does not exist")
        self.file_handle = open(self.file_path)
        self.file_handle.seek(0, 2)  # Seek to end
        self.last_position = self.file_handle.tell()
        logger.info(f"Opened file {self.file_path} for tailing")

    def read_batch(self) -> list[dict[str, Any]]:
        if self.file_handle is None:
            self.open_file()

        lines = []
        current_position = self.file_handle.tell()

        # Check if file has been truncated or rotated
        if current_position < self.last_position:
            logger.warning(
                "File appears to have been truncated or rotated, resetting position"
            )
            self.file_handle.seek(0)
            self.last_position = 0

        for _ in range(self.batch_size):
            line = self.file_handle.readline()
            if not line:
                break
            try:
                data = json.loads(line.strip())
                lines.append(data)
            except json.JSONDecodeError as e:
                logger.error(f"Failed to parse JSON line: {line.strip()}, error: {e}")
                continue

        self.last_position = self.file_handle.tell()
        return lines

    def close(self):
        if self.file_handle:
            self.file_handle.close()
            self.file_handle = None
            logger.info(f"Closed file {self.file_path}")
