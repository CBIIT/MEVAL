# MDF Enforced Validator and Loader (MEVAL)
MDF Enforced Validator and Loader is a lightweight and modular framework designed to ensure data integrity through MDF enforced validation and seamless data ingestion into the graph database. This repository serves as the source code for MEVAL, providing tools, scripts, and workflows to validate and load data to graph database using Graph Model Description Format (MDF) as the source of accessing model features.

## Core Modules

MEVAL includes three core classes under the `meval` package that work together to support model-aware validation and graph loading workflows:

### `ModelParser` (`meval/parser.py`)
`ModelParser` wraps `bento_mdf.MDFReader` and provides easy access to MDF model metadata.
It is used to inspect node definitions, key properties, required fields, parent-child relationships, property types, and permissible values.
This class is the model introspection layer used by both validation and loading logic.

### `Loader` (`meval/loader.py`)
`Loader` handles graph database ingestion for TSV data files.
It reads files in chunks, prepares node properties and relationships from each chunk, and performs upsert operations (MERGE semantics) for nodes and edges.
It also includes helper methods for index creation, duplicate cleanup, and graph maintenance tasks such as finding floating/orphan nodes (nodes without a path to a root node, such as study/program node).

### `Validator` (`meval/validator.py`)
`Validator` enforces MDF-based data quality checks before loading.
It validates TSV file format, validates record-level values against model constraints, checks relationship consistency across files, and supports unique-entry checks.
It also provides utilities such as deterministic UUID generation and adding UUID columns to TSV files.

## Installation

### Prerequisites
- Python 3.13

### Install from source
1. Clone this repository.
2. Create and activate a virtual environment.
3. Install dependencies:
   - `pip install -r requirements_python3.13.txt`
4. Install the package:
   - `pip install .`


