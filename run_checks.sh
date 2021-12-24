#!/bin/bash -e

python -m pylint saq/ tests/ setup.py
python -m black --check saq/ tests/ setup.py
python -m unittest
