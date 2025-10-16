import argparse
import subprocess
import sys
from pathlib import Path
import time


def run_stub_file_mode(engine_cmd=None):
    # either run provided engine and redirect to sensor.json, or run Python stub
    root = Path(__file__).resolve().parent.parent
    sensor_path = root / 'sensor.json'
    if engine_cmd:
        # spawn engine and write stdout to sensor.json
        with subprocess.Popen(engine_cmd, stdout=subprocess.PIPE, shell=isinstance(engine_cmd, str)) as p:
            try:
                for line in p.stdout:
                    sensor_path.write_text(line.decode('utf-8'))
            except KeyboardInterrupt:
                p.terminate()
    else:
        # run python stub
        subprocess.run([sys.executable, str(root / 'sensor_stub.py')])


def run_stdout_mode(engine_cmd=None, engine_args=None):
    # Launch UI pointing to engine stdout
    root = Path(__file__).resolve().parent.parent
    if engine_args:
        subprocess.Popen([sys.executable, str(root / 'ui' / 'banner.py'), '--engine-args', *engine_args])
    else:
        subprocess.Popen([sys.executable, str(root / 'ui' / 'banner.py'), '--engine', engine_cmd])


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--mode', choices=['file', 'stdout', 'stub'], default='stub')
    parser.add_argument('--engine', help='Engine command or path (string, shell executed)')
    parser.add_argument('--engine-args', nargs='+', help='Engine command and args as list')
    args = parser.parse_args()

    if args.mode == 'file':
        run_stub_file_mode(engine_cmd=args.engine)
    elif args.mode == 'stdout':
        if not args.engine and not args.engine_args:
            print('stdout mode requires --engine or --engine-args')
            sys.exit(1)
        run_stdout_mode(engine_cmd=args.engine, engine_args=args.engine_args)
    else:
        # stub mode: start sensor stub and UI
        root = Path(__file__).resolve().parent.parent
        subprocess.Popen([sys.executable, str(root / 'sensor_stub.py')])
        subprocess.Popen([sys.executable, str(root / 'ui' / 'banner.py')])


if __name__ == '__main__':
    main()
