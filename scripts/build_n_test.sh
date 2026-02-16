#!/bin/sh
set -e

flake8 --config=flake8.cfg
python -m pytest tests/ --tb=short -q
