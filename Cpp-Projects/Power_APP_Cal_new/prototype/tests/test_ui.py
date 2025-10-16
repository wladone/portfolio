import json
from pathlib import Path
import time

from pytestqt.qtbot import QtBot
from prototype.ui.banner import Banner


def test_banner_receives_sample(qtbot: QtBot):
    # ensure a sample file exists
    root = Path(__file__).resolve().parent.parent
    sample = {
        'timestamp': '2025-09-15T12:00:00',
        'cpu_percent': 3.2,
        'ram_percent': 40.1,
        'gpu_percent': 0.0,
        'temps': {'cpu_package': 42.0},
        'process_top': []
    }
    (root / 'sensor.json').write_text(json.dumps(sample))

    banner = Banner()
    qtbot.addWidget(banner)
    banner.show()

    # wait until the labels reflect data (not default placeholders)
    qtbot.waitUntil(lambda: banner.cpu_label.text() != 'CPU: --%' and 
                              banner.ram_label.text() != 'RAM: --%', timeout=3000)

    # check that labels updated
    assert 'CPU' in banner.cpu_label.text()
    assert 'RAM' in banner.ram_label.text()
    banner.close()
