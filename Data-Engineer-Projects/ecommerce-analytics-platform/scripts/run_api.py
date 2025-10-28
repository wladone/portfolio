"""Run the API with proper signal handling"""

import signal
import sys

from uvicorn import Config, Server


def handle_exit(signum, frame):
    sys.exit(0)


if __name__ == "__main__":
    signal.signal(signal.SIGINT, handle_exit)
    signal.signal(signal.SIGTERM, handle_exit)

    config = Config(
        "backend.app.main:app",
        host="127.0.0.1",
        port=8000,
        workers=1,
        reload=False,
        access_log=False,
    )

    server = Server(config)
    server.run()
