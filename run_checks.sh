#!/bin/bash -e

python -m ruff check saq/ tests/ setup.py
python -m black --check saq/ tests/ setup.py
python -m mypy saq/ tests/ setup.py
python -m unittest
