#!/bin/bash

set -e

python3.9 -m venv .venv
source .venv/bin/activate

pip install --upgrade pip
pip install pip-tools
pip install -r requirements/requirements.txt

echo "Activate the python environment with source .venv/bin/activate"
