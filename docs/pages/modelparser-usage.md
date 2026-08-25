---
layout: default
title: ModelParser Usage
permalink: /modelparser/usage/
---
# ModelParser Usage

`ModelParser` (`meval/parser.py`) is a higher-level wrapper around `bento_mdf.MDFReader` that offers direct and easy access to MDF model features — nodes, properties, edges, and their attributes — without needing to work with the underlying `bento_mdf.MDFReader` objects directly. `Validator` and `Loader` both use a `ModelParser` instance to interpret the model when validating or loading data.

## Instantiation

```python
from meval.parser import ModelParser

model_parser = ModelParser(
    model_file="model.yml",
    props_file="model-props.yml",
    handle="your_project",  # optional model/commons acronym
)
```

- `model_file` — path to the MDF model YAML file (nodes/edges).
- `props_file` — path to the MDF properties YAML file (property definitions).
- `handle` — optional model name/acronym (e.g. `"ccdi_dcc"`, `"icdc"`).

The parsed model is stored on `model_parser.model` (a `bento_mdf.MDFReader.model` model object) and is used internally by all methods below.

## Node introspection

- `get_node_list()` → list of all node (label) names in the model.
- `get_node_props_list(node_name)` → list of property names defined on a node.
- `get_node_props_list_required(node_name)` → list of required property names on a node.
- `get_node_props_if_list_type(node_name)` → list of property names whose value domain is `list` (multi-valued).
- `get_node_key_prop(node_name)` → the key (unique identifying) property name for a node.

```python
model_parser.get_node_list()
# ['study', 'participant', 'sample', ...]

model_parser.get_node_props_list_required("participant")
model_parser.get_node_key_prop("participant")  # e.g. "participant_id"
```

## Property introspection

- `get_prop_attr_dict(node_name, prop_name)` → full attribute dictionary for a property.
- `get_prop_type(node_name, prop_name)` → the property's value domain (e.g. `"string"`, `"number"`, `"integer"`, `"list"`, `"value_set"`).
- `get_permissible_values(node_name, prop_name)` → list of permissible values for a `value_set`-type property (or `None` if not applicable/defined).
- `if_prop_required(node_name, prop_name)` → whether the property is required.
- `if_prop_key(node_name, prop_name)` → whether the property is the node's key property.
- `if_prop_nullable(node_name, prop_name)` → whether the property allows null/empty values.
- `if_prop_strict(node_name, prop_name)` → whether the property must strictly match its defined permissible values.
- `if_prop_list(node_name, prop_name)` → whether the property is list type (multi-valued).

```python
model_parser.get_prop_type("participant", "sex")           # e.g. "value_set"
model_parser.get_permissible_values("participant", "sex")  # ["Male", "Female", ...]
model_parser.if_prop_required("participant", "participant_id")  # True
```

All of the above raise `KeyError` if the node and/or property is not found in the model, and `ValueError` (via `get_prop_attr_dict`, `get_prop_type`, `get_permissible_values`, and the `if_prop_*` methods) if the node exists but does not have the given property.

## Node relationships (edges)

- `get_parent_nodes(node_name)` → list of parent node names for a node.
- `get_child_nodes(node_name)` → list of child node names for a node.
- `get_root_node()` → the single root node name in the model (a node with no parents, typically `study`). Raises `ValueError` if none is found.
- `if_root_node(node_name)` → whether a node has no parent nodes.
- `if_leaf_node(node_name)` → whether a node has no child nodes.
- `get_all_edge_triplets()` → list of all `(handle, edge_src, edge_dst)` triplets in the model.
- `get_edge_multiplicity(edge_src, edge_dst)` → multiplicity of the edge between two nodes (e.g. `"many_to_one"`, `"one_to_many"`).
- `get_edge_handle(edge_src, edge_dst)` → the relationship handle/type name between two nodes.

```python
model_parser.get_parent_nodes("sample")   # ["participant"]
model_parser.get_root_node()              # "study"
model_parser.get_edge_handle(edge_src="sample", edge_dst="participant")  # "of_participant"
model_parser.get_edge_multiplicity(edge_src="sample", edge_dst="participant")  # "many_to_one"
```

`get_edge_multiplicity` and `get_edge_handle` raise `KeyError` if no edge exists between the given source and destination nodes.
