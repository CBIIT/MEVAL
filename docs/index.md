---
layout: default
---

# MDF Enforced Validator and Loader

Welcome to the **MDF Enforced Validator and Loader** ([MEVAL](https://github.com/CBIIT/MEVAL)) documentation!

## What is MEVAL?

**MDF Enforced Validator and Loader** is a lightweight and modular framework designed to ensure data integrity through MDF enforced validation and seamless data ingestion into the graph database. This repository serves as the source code for MEVAL, providing tools, scripts, and workflows to validate and load data to graph database using Graph Model Description Format (MDF) as the source of accessing model features.

## What is MDF and why MDF?

MDF stands for **Model Description Format**, which allows a user to provide a very simple, human-readable description of an overall property graph model. `MDF serves as the source of truth for accessing model features` and avoids the need for users to parse models using custom parsers. Custom parsers often cause discrepancies in model reading between projects.

More detailed documentation on MDF can be found [HERE](https://cbiit.github.io/bento-mdf/example.html)

## Core modules in MEVAL

MEVAL currently contains three core modules (`ModelParser`, `Validator`, and `Loader`) that work together to support model-aware validation and graph loading workflows.

### [ModelParser]({{ site.baseurl }}{% link pages/modelparser-usage.md %}) (meval/parser.py)

**ModelParser** wraps a `bento_mdf.MDFReader` instance (an MDF instance) and provides easy access to MDF model metadata.

It is used to inspect node definitions, key properties, required properties, parent-child relationships, property types, and permissible values of enumerate properties. This class is the model introspection layer used by both validation and loading logic.

### [Validator]({{ site.baseurl }}{% link pages/validator-usage.md %}) (meval/validator.py)

**Validator** enforces MDF-based data quality checks before loading. It contains functions to perform format validation of submission files, validate record-level values against model constraints, check relationship consistency across files, and support unique-entry checks.
It also provides utilities such as deterministic UUID generation and adding UUID columns to TSV files.

In addition, the `Validator` class can also be used to validate a set of loading files against an existing graph database. Users are able to get projected database changes before loading.

### [Loader]({{ site.baseurl }}{% link pages/loader-usage.md %}) (meval/loader.py)

**Loader** handles graph database ingestion for TSV data files. It reads files in chunks, prepares node properties and relationships from each chunk, and performs loading operations (MERGE semantics) for nodes and edges. It also includes helper methods for index creation, duplicate cleanup, and graph maintenance tasks such as finding floating/orphan nodes (nodes without a path to a root node, such as study/program node).

Currently, the `Loader` class is only able to load data in `Upsert` mode. Upsert (a blend of "Update" + "Insert") is a loading mode where, for each incoming record, the database inserts a new element if it doesn't already exist, or updates the existing one if it does—based on a matching key or identity. MEVAL uses the value of a universally unique identifier (UUID) property for record identification purposes.
