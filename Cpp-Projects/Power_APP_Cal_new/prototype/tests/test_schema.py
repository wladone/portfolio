import json
from pathlib import Path
import jsonschema


def test_sensor_json_matches_schema():
    root = Path(__file__).resolve().parent.parent
    schema = json.loads((root / 'schema' / 'sensor.schema.json').read_text())
    data = json.loads((root / 'sensor.json').read_text())
    jsonschema.validate(instance=data, schema=schema)
