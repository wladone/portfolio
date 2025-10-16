import socket
import threading
import json
from pathlib import Path


def client_handler(conn, addr, path: Path):
    # send last known sample then keep the connection open
    try:
        if path.exists():
            conn.sendall((path.read_text() + "\n").encode('utf-8'))
        while True:
            data = conn.recv(1024)
            if not data:
                break
    finally:
        conn.close()


def run_server(host='127.0.0.1', port=20123, path=None):
    path = Path(path) if path else Path(
        __file__).resolve().parent.parent / 'sensor.json'
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind((host, port))
    sock.listen(5)
    try:
        while True:
            conn, addr = sock.accept()
            threading.Thread(target=client_handler, args=(
                conn, addr, path), daemon=True).start()
    finally:
        sock.close()


if __name__ == '__main__':
    run_server()
