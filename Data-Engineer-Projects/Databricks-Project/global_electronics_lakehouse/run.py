#!/usr/bin/env python3
"""
Simple runner script for the Global Electronics Lakehouse Dashboard
"""

import subprocess
import sys
import os


def main():
    """Run the Streamlit dashboard"""
    try:
        # Change to the project root directory to allow module imports
        project_dir = os.path.dirname(__file__)
        os.chdir(project_dir)

        # Add project root to Python path for relative imports
        env = os.environ.copy()
        if 'PYTHONPATH' in env:
            env['PYTHONPATH'] = project_dir + os.pathsep + env['PYTHONPATH']
        else:
            env['PYTHONPATH'] = project_dir

        # Run streamlit with file path
        cmd = [sys.executable, '-m', 'streamlit', 'run', 'dashboard/app.py']
        subprocess.run(cmd, check=True, env=env)

    except subprocess.CalledProcessError as e:
        print(f"Error running dashboard: {e}")
        sys.exit(1)
    except KeyboardInterrupt:
        print("\nDashboard stopped by user")
        sys.exit(0)


if __name__ == "__main__":
    main()
