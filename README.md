# MDF Enforced Validator and Loader (MEVAL)
MDF Enforced Validator and Loader is a lightweight and modular framework designed to ensure data integrity through MDF enforced validation and seamless data ingestion into the graph database. This repository serves as the source code for MEVAL, providing tools, scripts, and workflows to validate and load data to graph database using Graph Model Description Format (MDF) as the source of accessing model features.

## Core Modules

MEVAL includes three core classes under the `meval` package that work together to support model-aware validation and graph loading workflows:

### `ModelParser` (`meval/parser.py`)
`ModelParser` wraps `bento_mdf.MDFReader` and provides easy access to MDF model metadata.(`bento_mdf` repository: https://github.com/CBIIT/bento-mdf)

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

### Install from PyPI
1. Create and activate a virtual environment with Python 3.13+.
2. Install ctos-meval
```Python
pip install ctos-meval
```

## Example Usage

The sections below provide separate examples for each core module.

### Shared setup (imports and initialization)

```python
from bento_mdf import MDFReader
from neo4j import GraphDatabase

from meval.loader import Loader
from meval.parser import ModelParser
from meval.validator import Validator

model_file = "tests/test_files/ccdi-dcc-model-test.yml"
props_file = "tests/test_files/ccdi-dcc-model-props-test.yml"

# Initialize a ModelParser instance
model_parser = ModelParser(
   model_file=model_file,
   props_file=props_file,
   handle="test",
)

# Initialize a Validator instance
mdf = MDFReader(model_file, props_file, handle="test")
validator = Validator(mdf=mdf)

# Initialize a Loader instance
driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "your_password"))
loader = Loader(driver=driver)
```

### 1. ModelParser module examples (`meval/parser.py`)

#### Basic model inspection

```python
# list all node types in the model
node_list = model_parser.get_node_list()
print(node_list)

# get a full list of methods
dir(model_parser)

# inspect one node
node_name = "participant"
# list all properties under node participant
model_parser.get_node_props_list(node_name)
# list all the required properties under node participant
model_parser.get_node_props_list_required(node_name)
# get key prop of node participant
model_parser.get_node_key_prop(node_name)
```

#### Property-level checks

```python
node_name = "participant"
prop = "sex_at_birth"

# get property type
model_parser.get_prop_type(node_name, prop)
# get permissible values
model_parser.get_permissible_values(node_name, prop)
# if property strict
model_parser.if_prop_strict(node_name, prop)
# if property required
model_parser.if_prop_required(node_name, prop)
# full metadata dict for a property
model_parser.get_prop_attr_dict(node_name=node_name, prop_name=prop)
```

#### Relationship and graph-shape helpers

```python
# get root node (no outgoing edges)
model_parser.get_root_node()
# check if root node
model_parser.if_root_node(node_name)
# check if leaf node (no edge that ends with the tested node)
model_parser.if_leaf_node(node_name)

# parent/child traversal
# get a list of nodes that node "participant" can point to
model_parser.get_parent_nodes(node_name)
# get a list of nodes that can have edge that ends with "participant"
model_parser.get_child_nodes(node_name)

# edge metadata
model_parser.get_all_edge_triplets()
# get edge multiplicity
model_parser.get_edge_multiplicity(edge_src=node_name, edge_dst="consent_group")
# get edge name/handle
model_parser.get_edge_handle(edge_src=node_name, edge_dst="consent_group")
```

### 2. Validator module examples (`meval/validator.py`)

#### Validate file format and record content

```python
participant_file = "tests/test_files/participant_test_without_uuid.tsv"

# format validation for one file
format_errors = validator.validate_tsv_format(participant_file)

# format validation for multiple files
format_errors_by_file = validator.validate_tsv_files_format([
   participant_file,
   "tests/test_files/survival_test.tsv",
])

# record-level MDF validation (returns only invalid rows)
invalid_records = validator.validate_tsv_records(
   file_path=participant_file,
   id_field="guid",
   delimiter=";",
)
```

#### Relationship and uniqueness checks

```python
rel_files = [
   "tests/test_files/rel_test_files/test_rel_study.tsv",
   "tests/test_files/rel_test_files/test_rel_participant.tsv",
   "tests/test_files/rel_test_files/test_rel_consent_group.tsv",
   "tests/test_files/rel_test_files/test_rel_generic_file.tsv",
]

# cross-file relationship validation
rel_errors = validator.validate_tsv_rels(rel_files, rel_delimiter=";")

# detect duplicate key-property entries within each node type
duplicated_entries = validator.validate_tsv_uniq_entry(rel_files)
```

#### UUID and file utilities

```python
# add guid + converted relationship guid columns to a TSV
Validator.add_uuid_to_tsv_file(
   file_path="tests/test_files/participant_test_without_uuid.tsv",
   project_name="ccdi_dcc",
   mdf=mdf,
   output_file_path="/tmp/participant_with_guid.tsv",
   uuid_column="guid",
   delimiter=";",
)

# recursively find all .tsv files in a folder
tsv_paths = Validator.find_tsv_files("tests/test_files", recursive=True)
```

### 3. Loader module examples (`meval/loader.py`)

#### Upsert nodes and relationships
MEVAL currently contains functions to `Upsert` data into a graph database.

`Upsert` loading means each record/edge is either inserted if it doesn't already exist or updated if it does. Both `data node` and `edge` are entities in a graph database. No deletion of data node or edge is performed during data loading in `Upsert` mode.

**Note**: Relationships can only be created if data nodes at two ends have been created. That's why data nodes are loaded first before relationships.

```python
# upsert node properties from a file
node_summary = loader.upsert_file_records(
   file_path="tests/test_files/rel_test_files/test_rel_participant.tsv",
   model_parser=model_parser,
   id_field="guid",
   chunk_size=3000,
   delimiter=";",
)
print(node_summary)

# upsert relationships from a file
rel_summary = loader.upsert_file_relationships(
   file_path="tests/test_files/rel_test_files/test_rel_participant.tsv",
   processed_rel_dict = {},
   model_parser=model_parser,
   id_field="guid",
   delimiter=";",
)
print(rel_summary)
```

#### Index and graph maintenance helpers

```python
# create label-property index pairs for an entire model
created_indexes = loader.create_index(model_parser=model_parser, property_name="guid")

# drop a single label-property pair index
index_list = [{"label":"study", "property":"guid"}]
loader.drop_index(index_list)
# drop all indexes in a graph database
loader.drop_all_indexes()

# check graph health and clean floating nodes
floating_ids = loader.find_nodes_without_path_to_root(root_node_label="study")

loader.close()
```


