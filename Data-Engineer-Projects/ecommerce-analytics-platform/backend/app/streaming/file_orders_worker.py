import asyncio
import logging
import sys
from pathlib import Path

from .filetail import FileTailReader
from .orders_worker import OrdersWorker
from .settings import StreamingSettings

logger = logging.getLogger(__name__)


class FileOrdersWorker:
    def __init__(self, settings: StreamingSettings, file_path: str):
        self.settings = settings
        self.file_path = Path(file_path)
        self.reader = FileTailReader(
            str(self.file_path), batch_size=settings.batch_size
        )
        self.orders_worker = OrdersWorker(settings)

    async def tail(self):
        """Process all existing lines in the file and stop."""
        self.reader.open_file()
        try:
            while True:
                batch = self.reader.read_batch()
                if not batch:
                    break
                await self.orders_worker.process_batch(batch)
            logger.info(f"Finished processing file: {self.file_path}")
        finally:
            self.reader.close()

    async def watch(self):
        """Continuously monitor the file for new lines."""
        self.reader.open_file()
        try:
            while True:
                batch = self.reader.read_batch()
                if batch:
                    await self.orders_worker.process_batch(batch)
                else:
                    await asyncio.sleep(1)  # Wait before checking again
        except KeyboardInterrupt:
            logger.info("Stopping file watch")
        finally:
            self.reader.close()


def main():
    if len(sys.argv) < 3:
        print(
            "Usage: python -m backend.app.streaming.file_orders_worker <tail|watch> <file_path>"
        )
        sys.exit(1)

    mode = sys.argv[1]
    file_path = sys.argv[2]

    settings = StreamingSettings()
    worker = FileOrdersWorker(settings, file_path)

    if mode == "tail":
        asyncio.run(worker.tail())
    elif mode == "watch":
        asyncio.run(worker.watch())
    else:
        print("Invalid mode. Use 'tail' or 'watch'")
        sys.exit(1)


if __name__ == "__main__":
    main()
