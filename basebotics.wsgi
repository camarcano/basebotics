import sys
import os
from pathlib import Path

# Path to your Flask app
project_dir = '/var/www/basebotics'
venv_dir = '/var/www/basebotics/venv'

# Add the app's directory to the Python path
sys.path.insert(0, project_dir)

# Activate the virtual environment
venv_python = Path(venv_dir) / 'bin' / 'python3.12'
if not venv_python.exists():
    raise RuntimeError(f"Virtual environment Python not found at {venv_python}")
venv_site_packages = Path(venv_dir) / 'lib' / 'python3.12' / 'site-packages'
sys.path.insert(1, str(venv_site_packages))

# Import the Flask app
from app import create_app

application = create_app()
