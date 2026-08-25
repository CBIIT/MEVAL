---
layout: default
title: Install MEVAL
permalink: /install/
---
# Install MEVAL

## Prerequisites
- Python 3.13

## Dependencies
MEVAL relies on the following packages:
```
bento-mdf>=0.13.3
boto3>=1.36.11,<2.0
pandas>=2.2.3,<3.0
neo4j-viz>=1.0.0,<2.0
neo4j>=6.2.0,<7.0
tabulate>=0.9.0,<1.0
```
These are installed automatically when installing via `pip` (PyPI) or `uv`.

## Create a Virtual Environment

We recommend creating an isolated Python virtual environment when you isntall MEVAL, so that your installation doesn't encounter version conflicts with other packages in your existing environment. 

### Using `venv`
1. Create and activate a virtual environment with Python 3.13+.
```bash
python3.13 -m venv .venv
source .venv/bin/activate
```

### Using `uv`
[`uv`](https://docs.astral.sh/uv/) is a fast Python package and project manager that can create virtual environments and install dependencies.

1. Install `uv` (if not already installed):
```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```
2. Create a virtual environment with Python 3.13:
```bash
uv venv --python 3.13
source .venv/bin/activate
```
3. Install MEVAL (and its dependencies) into the environment:
```bash
uv pip install ctos-meval
```

## Install from PyPI
1. Create and activate a virtual environment with Python 3.13+ (see above).
2. Install ctos-meval
```bash
pip install ctos-meval
```

## Install from Repo Download
1. Clone the MEVAL repository (including submodules):
```bash
git clone --recurse-submodules https://github.com/CBIIT/MEVAL.git
cd MEVAL
```
2. Create and activate a virtual environment with Python 3.13+ (see above).
3. Install the dependencies:
```bash
pip install -r requirements_python3.13.txt
```
4. Install MEVAL in editable mode:
```bash
pip install -e .
```
