import json
import random
import time
from pathlib import Path


def run_stub(path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    while True:
        sample = {
            'timestamp': time.strftime('%Y-%m-%dT%H:%M:%S'),
            'cpu_percent': round(random.uniform(1, 20), 1),
            'ram_percent': round(random.uniform(20, 70), 1),
            'gpu_percent': round(random.uniform(0, 10), 1),
            'temps': {'cpu_package': round(random.uniform(35, 65), 1)},
            'process_top': []
        }
        path.write_text(json.dumps(sample))
        time.sleep(0.8)


if __name__ == '__main__':
    p = Path(__file__).resolve().parent / 'sensor.json'
    run_stub(p)
