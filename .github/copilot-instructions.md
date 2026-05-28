# Copilot Instructions for MEVAL

## Project Overview

**MEVAL** (MDF Enforced Validator and Loader) is a lightweight, modular Python framework for validating and loading biomedical data into a graph database (Memgraph/Neo4j). It uses **Graph Model Description Format (MDF)** as the authoritative source for data model structure, enforcing model constraints during both validation and ingestion. The project targets CBIIT data commons (CCDI, ICDC, CDS, C3DC, CTDC, etc.).

## Repository Structure

```
MEVAL/
├── src/                        # Core library modules
│   ├── loader.py               # Loader class: reads TSV files and upserts nodes/relationships to graph DB
│   ├── validator.py            # Validator class: validates TSV files against the MDF model
│   ├── parser.py               # ModelParser class: wraps bento-mdf MDFReader for easy model access
│   └── utils.py                # AWS S3 utilities (upload/download), secrets manager helpers
├── workflows/
│   └── prefect/                # Prefect flow entrypoints (deployed via prefect.yaml)
│       ├── upsert_workflow.py          # Main data-loading flow (UPSERT semantics)
│       ├── validate_tsv_files.py       # Validation-only flow
│       ├── add_uuid_to_files.py        # UUID generation flow
│       ├── delete_database_study_subgraph.py  # Delete subgraph flow
│       ├── find_anchored_traversals.py # Graph traversal visualization flow
│       ├── find_db_floating_nodes.py   # Find/delete orphan nodes flow
│       └── wipe_database.py            # Wipe entire database flow
├── libs/
│   └── prefect-toolkit/        # Git submodule (CBIIT/prefect-toolkit) — provides download_model_files()
├── tests/
│   ├── test_loader.py          # Unit tests for Loader
│   ├── test_validator.py       # Unit tests for Validator and ModelParser
│   └── test_files/             # Fixture TSV and YAML model files for tests
├── prefect.yaml                # Prefect deployment configuration
├── requirements_python3.13.txt # Python dependencies (Python 3.13)
└── .gitmodules                 # Submodule: libs/prefect-toolkit -> github.com/CBIIT/prefect-toolkit
```

## Core Classes and Their Roles

### `src/parser.py` — `ModelParser`
- Wraps `bento_mdf.MDFReader` and provides high-level access to the MDF data model.
- Key methods: `get_node_list()`, `get_node_props_list()`, `get_node_key_prop()`, `get_parent_nodes()`, `get_child_nodes()`, `get_root_node()`, `get_edge_multiplicity()`, `get_edge_handle()`, `if_prop_required()`, `if_prop_list()`, `if_prop_strict()`.
- Instantiation: `ModelParser(model_file="model.yml", props_file="props.yml", handle="ccdi_dcc")`.
- `handle` is the model/commons acronym (e.g., `"ccdi_dcc"`, `"ccdi"`, `"icdc"`).

### `src/validator.py` — `Validator`
- Instantiation: `Validator(mdf: MDFReader)`.
- Key methods:
  - `validate_tsv_format(file_path)` → list of format errors per file
  - `validate_tsv_files_format(file_path_list)` → dict of file → format errors
  - `validate_tsv_records(file_path, ...)` → list of per-row validation results
  - `validate_tsv_rels(file_path_list, rel_delimiter)` → dict of file → relationship errors
  - `validate_tsv_uniq_entry(file_path_list)` → list of duplicate entries
  - `add_uuid_to_tsv_file(file_path, project_name, mdf, output_file_path, uuid_column)` → writes TSV with UUID column added
  - `generate_uuid5(project_name, subgraph_value, record_type, record_key_value, delimiter)` → deterministic UUID5
  - `find_tsv_files(folder_path)` → list of PosixPath for all TSVs in folder

### `src/loader.py` — `Loader`
- Instantiation: `Loader(driver: GraphDatabase.driver)`.
- Key methods: `upsert_file_records(...)`, `upsert_file_relationships(...)`, `create_index(...)`, `find_nodes_without_path_to_root(root_node_label)`, `delete_nodes_by_internal_id(...)`, `delete_nodes_by_prop_value(...)`.
- Static helpers: `chunks(lst, size)`, `read_file_in_chunks(file_path, ...)`, `generate_chunk_records(chunk, model_parser, ...)`, `generate_chunk_relationships(chunk, model_parser, ...)`, `remove_chunk_duplicates(...)`.

### `src/utils.py`
- AWS utilities: `file_dl_s3()`, `file_ul_s3()`, `folder_dl_s3()`, `get_secret()`, `get_secret_centralized_worker()`, `parse_file_url()`, `set_s3_resource()`.
- Supports LocalStack for local development via `LOCALSTACK_ENDPOINT_URL` environment variable.

## Data Model and TSV Format Conventions

- TSV files **must** contain a `type` column identifying the node type (e.g., `participant`, `survival`).
- The `guid` column (configurable via `uuid_field`) is the unique identifier for each record.
- Relationship columns follow the pattern `<parent_node_type>.<parent_key_prop>` (e.g., `participant.guid`, `consent_group.consent_group_id`).
- Multi-valued fields (list-type properties and multi-parent relationships) use `;` as delimiter by default.
- A `subgraph` column (optional, configurable via `subgraph_col`) indicates the study/program context used for UUID generation.
- The `type` column and relationship columns are **not** loaded as node properties — they are stripped during record preparation.

## MDF and bento-mdf Dependency

- MDF model files come in pairs: `*-model.yml` (nodes/edges) and `*-model-props.yml` (property definitions).
- The `bento_mdf` library (`bento-mdf==0.13.1`) provides `MDFReader`, `MDF`, and `MDFDataValidator`.
- Model files for production workflows are downloaded at runtime from GitHub model repos via `libs/prefect-toolkit/workflow/validate_submission.py:download_model_files(commons_acronym, tag)`.
- Test fixtures use local YAML files in `tests/test_files/` (e.g., `ccdi-dcc-model-test.yml`, `ccdi-dcc-model-props-test.yml`).

## Prefect Workflows

- All flows are defined in `workflows/prefect/` and deployed via `prefect.yaml`.
- Deployed on a Prefect work pool: `ccdi-dcc-16gb-prefect-3.4.19-python3.13` (Python 3.13, 16 GB).
- At runtime, the repo is cloned via `prefect.deployments.steps.git_clone` (with submodules) and deps installed from `requirements_python3.13.txt`.
- Submodule `libs/prefect-toolkit` must be initialized: `git submodule update --init --recursive`.
- Flows import from `libs/prefect-toolkit` using a `sys.path.insert(0, "./libs/prefect-toolkit")` pattern.
- The main upsert flow (`upsert_files`) takes AWS credentials from Secrets Manager, downloads model files, creates DB indexes, upserts nodes then relationships, and optionally detects/deletes floating nodes.
- Supported commons acronyms: `"ccdi"`, `"icdc"`, `"cds"`, `"c3dc"`, `"ctdc"`, `"ccdi_dcc"`, `"popsci"`.

## AWS Infrastructure

- Graph DB (Memgraph/Neo4j) connection URI and credentials are stored in **AWS Secrets Manager** and retrieved at flow runtime.
- Data files (TSVs) are read from **S3** and output results (summaries, logs, validation JSON) are written back to **S3**.
- `us-east-1` region is used throughout.
- LocalStack (`LOCALSTACK_ENDPOINT_URL`) is supported for local S3 development.

## Dependencies (Python 3.13)

```
bento-mdf==0.13.1
boto3==1.36.11
pandas==2.3.3
neo4j-viz==1.0.0
tabulate==0.9.0
```

Additional runtime dependencies come from `libs/prefect-toolkit` (notably `prefect`, `neo4j`).

## Testing

- Tests are in `tests/` using Python's built-in `unittest` framework.
- Run all tests from the repo root:
  ```bash
  python -m pytest tests/
  # or
  python -m unittest discover tests/
  ```
- `test_validator.py` requires the local test fixture YAML files at `tests/test_files/ccdi-dcc-model-test.yml` and `tests/test_files/ccdi-dcc-model-props-test.yml`. If these cannot be loaded, tests are skipped.
- `test_loader.py` uses `unittest.mock.MagicMock` for the Neo4j driver — no live database required.
- Tests add the project root to `sys.path` manually (no `__init__.py` or installed package).

## Common Patterns and Conventions

- **`type` column**: Always the first column in TSV files; used to determine node type for each row.
- **Chunked processing**: Large TSV files are read in chunks (default 3000 rows) via `Loader.read_file_in_chunks()` / `pd.read_csv(..., chunksize=...)`.
- **UUID5 generation**: UUIDs are deterministically generated as `uuid5(project_namespace, "{subgraph}::{node_type}::{key_value}")`. The project namespace itself is derived from `md5(project_name)`.
- **Encoding detection**: Files are auto-detected as `utf-8` or `cp1252`.
- **NaN handling**: Empty strings in TSVs are treated as `NaN`; `NaN` values are excluded from loaded records.
- **List-type properties**: Stored as Python lists; split on the configured delimiter (default `;`) with whitespace stripped.
- **Relationship upsert**: Uses MERGE semantics in Cypher to avoid duplicates; existing relationships are not deleted during upsert.
- **Logging**: Dual logging — Prefect's `get_run_logger()` for the Prefect UI, and a `logging.FileHandler` (`upsert_logger`) for a persistent log file uploaded to S3 after the flow completes.

## Known Errors and Workarounds

- **Submodule not initialized**: If `from workflow.validate_submission import download_model_files` fails, ensure the submodule is initialized: `git submodule update --init --recursive`. The `libs/prefect-toolkit` directory must be non-empty.
- **`sys.path` injection**: Workflows add `./libs/prefect-toolkit` to `sys.path` at import time. When running workflows from a directory other than the repo root, this relative path may fail — use an absolute path or set `PYTHONPATH` instead.
- **`bento_mdf` version pinning**: `bento-mdf==0.13.1` is pinned; do not upgrade without verifying API compatibility, as the `MDFReader`, `MDF`, and `MDFDataValidator` APIs may change.
- **No `setup.py` / no package install**: The `src` module is not installed as a package. Scripts always rely on running from the repo root or inserting the root into `sys.path`.
