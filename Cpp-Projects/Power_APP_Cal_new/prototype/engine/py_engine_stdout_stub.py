import time
import json
import random
import sys
for i in range(6):
    sample = {
        'timestamp': time.strftime('%Y-%m-%dT%H:%M:%S'),
        'cpu_percent': round(random.uniform(1, 50), 1),
        'ram_percent': round(random.uniform(20, 80), 1),
        'gpu_percent': round(random.uniform(0, 10), 1),
        'temps': {'cpu_package': round(random.uniform(30, 90), 1)},
        'process_top': []
    }
    sys.stdout.write(json.dumps(sample) + '\n')
    sys.stdout.flush()
    time.sleep(0.6
               )
