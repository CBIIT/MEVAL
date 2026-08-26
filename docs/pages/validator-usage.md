---
layout: default
title: Validator Usage
permalink: /validator/usage/
---
# Validator Usage

`Validator` (`meval/validator.py`) enforces MDF-based data quality checks on submission TSV files. It supports two broad categories of validation:

- **Local validation** — checks a TSV file (or set of files) for internal consistency: file/column format, per-record property validity, cross-file relationship (linkage) validity, and duplicate key entries — without needing a database connection.
- **Validation against the database** — checks a TSV file against an existing graph database to determine whether records/relationships would be created vs. updated, and whether they are valid to load (e.g. relationship targets exist somewhere).

It also provides utilities for deterministic UUID5 generation and adding a `guid` column to TSV files before loading.

## Contents

- [Instantiation](#instantiation)
- [Local validation](#local-validation)
- [UUID generation](#uuid-generation)
- [Validation against an existing database](#validation-against-an-existing-database)
- [Notes](#notes)

## Instantiation

```python
from bento_mdf import MDFReader
from meval.validator import Validator

mdf = MDFReader("model.yml", "model-props.yml", handle="ccdi_dcc")
validator = Validator(mdf)
```

`validator.model` exposes the underlying MDF model, and `validator.record_validator` is the `bento_mdf.MDFDataValidator` instance used for record-level checks.

## Local validation

These methods only read TSV files and the MDF model — no database connection is required.

### `validate_tsv_format(file_path)`
Validates the structural format of a single TSV file: presence of a `type` column, no missing/mixed `type` values, `type` is a valid node in the model, all required properties are present as columns, unknown property columns (warning), and relationship columns (`<parent>.<parent_key_prop>`) are valid and complete for the node type. Returns a list of error/warning dicts (empty list if the file is valid).

### `validate_tsv_files_format(file_path_list)`
Runs `validate_tsv_format` over multiple files. Returns a dict of `{file_path: [errors...]}`, only including files that have at least one issue.

### `validate_tsv_records(file_path, subgraph_col=None, id_field=None, delimiter=";")`
Validates each row's property values (types, required-ness, permissible values) against the model using `MDFDataValidator`, one record at a time. Returns a list of `{"row": int, "is_valid": bool, "messages": {...}}` for invalid rows only.

### `validate_tsv_rels(file_path_list, rel_delimiter=";")`
Validates relationship columns across a set of TSV files that belong to the **same subgraph** (e.g. all files for one study). For each relationship column, confirms the column itself is valid for the model, that every non-empty node has at least one relationship value, and that every relationship value matches an existing key-property value in the corresponding parent-type file(s). Must be run **before** UUID5 generation, since it matches on the natural key column (e.g. `participant.participant_id`), not `guid`. Returns `{file_path: [issue...]}`.

### `validate_tsv_uniq_entry(file_path_list)`
Checks that key-property values are unique within each node type across a set of files from the same subgraph (duplicate key values under the same type + subgraph would collide to the same UUID5). Returns a list of duplicate-entry dicts.

```python
files = validator.find_tsv_files("submission_folder")
format_errors = validator.validate_tsv_files_format(files)
uniq_errors = validator.validate_tsv_uniq_entry(files)
rel_errors = validator.validate_tsv_rels(files)
record_errors = validator.validate_tsv_records("participant.tsv")
```

### Other local-validation helpers
- `validate_records(node_name, list_of_records)` / `validate_one_record(node_name, record)` — lower-level record validation against `MDFDataValidator` for a list or a single dict record.
- `record_prep(record_dict, mdf, subgraph_col=None, id_field=None, delimiter=";")` — strips `type`/id/relationship/subgraph keys and empty values from a record, and converts list-type/number-type property values, in preparation for validation.
- `if_rel_valid(child_type, mdf, rel_to_test)` — checks whether a `<parent>.<parent_key_prop>` relationship column name is valid for a given child node type.
- `get_rel_multiplicity(node_type, parent_node_type, mdf)` — returns the multiplicity (e.g. `"many_to_one"`, `"one_to_many"`) of the edge between two node types.
- `find_tsv_files(folder_path, recursive=True)` — finds all `.tsv` files under a folder.
- `file_type_read(file_path_list)` — categorizes a list of files by their `type` column value, returning `{type: [file_path, ...]}`.
- `create_subgraph_dict(file_folder_path)` — groups TSV files under a folder by their `subgraph` column value, returning `{subgraph_value: [file_path, ...]}`.
- `add_subgrapgh_value_to_tsv(file_path, subgraph_vlaue, output_file_path)` — writes a copy of a TSV file with the `subgraph` column set to a given value.

## UUID generation

### `generate_uuid5(project_name, subgraph_value, record_type, record_key_value, delimiter=";")`
Deterministically generates a UUID5 from `{subgraph_value}::{record_type}::{key_value}`, namespaced by `get_project_namespace(project_name)` (an MD5-derived UUID from the project/commons name). If `record_key_value` contains the `delimiter`, each part is hashed separately and the delimiter-joined UUID string is returned (used for multi-valued relationship columns).

### `add_uuid_to_tsv_file(file_path, project_name, mdf, output_file_path, uuid_column="guid", delimiter=";", subgraph_value=None)`
Reads a TSV file, generates a `guid` (or custom `uuid_column`) value for every record and for every relationship column (converting `<parent>.<parent_key_prop>` columns to `<parent>.guid`), then writes the result to `output_file_path` with the original relationship and `subgraph` columns removed. The subgraph value is taken from the `subgraph` column in the file unless explicitly passed via `subgraph_value`.

```python
validator.add_uuid_to_tsv_file(
    file_path="participant.tsv",
    project_name="ccdi",
    mdf=mdf,
    output_file_path="participant_with_uuid.tsv",
)
```

### `get_project_namespace(project_name)`
Returns the `uuid.UUID` namespace derived from `md5(project_name)`, used internally by `generate_uuid5`.

## Validation against an existing database

These methods compare submission files against records already stored in the graph database, to project what would change on load and to catch relationship targets that don't exist anywhere (neither in the database nor the submission set). They require a Neo4j/Memgraph `driver` and are designed to run **after** UUID5 generation (matching is done on `guid`, not the natural key).

### `validate_tsv_in_db(driver, tsv_file_path, tsv_id_set, mdf_instance, id_prop_name, delimiter=";", validation_mode="Upsert")`
The main entry point for DB validation of a single file. For each row it determines, based on `validation_mode`:

- `"New"` — the record must **not** already exist in the database.
- `"Update"` — the record **must** already exist in the database.
- `"Upsert"` — the record may or may not exist; existing property values not present in the file are preserved (not deleted).

For every relationship in the row, it checks whether the destination node exists in the database or in the current submission set (`tsv_id_set`, built via `build_tsv_id_set`); if neither, the row fails with an `invalid_edge_dst_node_not_found` error. Returns a tuple `(passed_row_list, failed_row_list, val_summary, validation_results)`, where `val_summary` includes `projected_changes_of_passed_rows` (counts of nodes/edges to be created, updated, or deleted).

```python
tsv_id_set = Validator.build_tsv_id_set(all_submission_files, id_field="guid")
passed, failed, summary, results = Validator.validate_tsv_in_db(
    driver=driver,
    tsv_file_path="participant_with_uuid.tsv",
    tsv_id_set=tsv_id_set,
    mdf_instance=mdf,
    id_prop_name="guid",
    validation_mode="Upsert",
)
```

### Supporting DB-validation helpers
- `build_tsv_id_set(tsv_file_list, id_field="guid")` — reads a set of files once and collects all `id_field` values into a set, for O(1) membership checks (used to test if a relationship's destination node is part of the current submission).
- `if_node_id_in_tsv_list(id_value, tsv_id_set)` — O(1) membership test against a set built by `build_tsv_id_set`.
- `if_record_exist_in_db(driver, id_prop_value, id_prop_name="guid", node_label=None)` / `if_file_records_exist_in_db(driver, file_path, id_prop_name, node_label)` — check whether one record, or every record in a file, already exists in the database (batched).
- `get_node_record_in_db(...)` / `get_file_records_in_db(...)` — fetch the current database property values for one record, or for every record in a file (batched), for use in `record_comparison`.
- `if_edge_exist_in_db(...)` / `if_parent_nodes_exist_in_db(...)` — check whether a relationship's destination (parent) node(s) already exist in the database.
- `get_record_outgoing_edges_in_db(...)` / `get_file_records_outgoing_edges_in_db(...)` — fetch existing outgoing relationships for a record, or for every record in a file (batched), to compare against relationships declared in the submission file.
- `record_comparison(record_file, record_db, compare_mode)` — diffs a file record against its database record, returning `{"added": ..., "removed": ..., "changed": ...}`. Under `"Upsert"` mode, `removed` is always empty since upsert does not delete missing properties; under `"Update"` mode, missing properties are reported as `removed`.
- `read_tsv_records_id`, `read_tsv_rels_id`, `read_full_records_in_tsv`, `read_record_by_row_in_tsv` — low-level generators/readers used internally to stream ids, relationships, and prepared records row-by-row for the DB validation pass.

## Notes

- Local validation methods (`validate_tsv_format`, `validate_tsv_records`, `validate_tsv_rels`, `validate_tsv_uniq_entry`, etc.) work on the natural key columns and should run **before** UUID5 generation.
- DB-validation methods (`validate_tsv_in_db` and its helpers) work on the `guid` column and should run **after** UUID5 generation (`add_uuid_to_tsv_file`).
- Encoding of TSV files (`utf-8` vs `cp1252`) is auto-detected via `check_encoding`, matching `Loader`'s behavior.
