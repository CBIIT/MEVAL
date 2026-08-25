---
layout: default
title: Prefect Workflow Deployment
permalink: /prefect/
---
# Prefect Workflows

The flows in `workflows/prefect/` inside MEVAL repo are the deployable Prefect entrypoints (see `prefect.yaml`) that wrap MEVAL's `ModelParser`, `Validator`, and `Loader` classes for use in Prefect Cloud. 

Any flows that require a database connection will retrieve credentials from AWS Secrets Manager (`db_account_id`, `db_creds_secret_name`, plus secret key names) and most download MDF model files at runtime via `libs/prefect-toolkit`'s `download_model_files(commons_acronym, tag)`.

## Contents
- [`prefect.yaml` deployment config](#prefectyaml-deployment-config)
  - [Deploying a workflow from the terminal](#deploying-a-workflow-from-the-terminal)
- [Workflow entrypoints](#workflow-entrypoints)
  - [`validate_tsv_files.py` — Validate TSV Files](#validate_tsv_filespy--validate-tsv-files)
  - [`add_uuid_to_files.py` — Add UUID to a set of TSV files](#add_uuid_to_filespy--add-uuid-to-a-set-of-tsv-files)
  - [`upsert_workflow.py` — Dataloading Upsert Workflow](#upsert_workflowpy--dataloading-upsert-workflow)
  - [`validate_submission_against_db.py` — Validate submission files against database](#validate_submission_against_dbpy--validate-submission-files-against-database)
  - [`find_db_floating_nodes.py` — Find/delete floating (orphan) nodes](#find_db_floating_nodespy--finddelete-floating-orphan-nodes)
  - [`precision_deletion_guid.py` — Precision Deletion Nodes](#precision_deletion_guidpy--precision-deletion-nodes)
  - [`delete_database_study_subgraph.py` — Delete Database Study Subgraph Flow](#delete_database_study_subgraphpy--delete-database-study-subgraph-flow)
  - [`find_anchored_traversals.py` — Find Anchored Traversals Flow](#find_anchored_traversalspy--find-anchored-traversals-flow)
  - [`wipe_database.py` — Wipe out Database Flow](#wipe_databasepy--wipe-out-database-flow)
- [Typical pipeline order](#typical-pipeline-order)

## `prefect.yaml` deployment config

`prefect.yaml` at the repo root defines how each flow below is deployed to Prefect Cloud. Each entry under `deployments` specifies:
- `entrypoint` — `<path/to/flow_file.py>:<flow_function_name>`, e.g. `workflows/prefect/upsert_workflow.py:upsert_files`.
- `parameters` — default parameter values for the deployment.
- `pull` — steps run before each flow run: `git_clone` (with `include_submodules: True`, since `libs/prefect-toolkit` is a submodule) followed by `pip_install_requirements` from `requirements_python3.13.txt`.
- `work_pool.name` — the Prefect work pool the flow runs on.

### Deploying a workflow from the terminal

With the `prefect` CLI installed and authenticated (`prefect cloud login` or `PREFECT_API_URL`/`PREFECT_API_KEY` set), deploy one of the named deployments from `prefect.yaml`:

```bash
# deploy a single named deployment (matches the `name:` field under `deployments`)
prefect deploy --name meval-dataloader
```

Once deployed, you will be able to find the deployed workflow under the `Deployment` tab of Prefect Cloud UI.

## Workflow entrypoints

### `validate_tsv_files.py` — Validate TSV Files
Runs the full **local** validation pipeline (`Validator`) against a set of TSV files for a study/program pulled from S3: TSV format check (always run), plus optional record check, relationship/linkage check, and unique-key check (toggle via `validation_items`). Validation summaries are uploaded back to S3. Include all files of a study/program in one run — validating a partial set can miss cross-file issues (e.g. broken relationships).

### `add_uuid_to_files.py` — Add UUID to a set of TSV files
Downloads a folder of TSV files from S3, generates a deterministic UUID5 (`guid` by default) for every record and relationship column using `Validator.add_uuid_to_tsv_file`, and uploads the resulting `*_added.tsv` files. Should be run after files have passed the `validate_tsv_files` pipeline, and requires a `subgraph_value` (e.g. a study accession) as part of the UUID namespace.

### `upsert_workflow.py` — Dataloading Upsert Workflow
Contains the two main data-loading flows:
- **`upsert_files`** — downloads model files and a single S3 TSV folder, creates DB indexes, upserts node records then relationships (`Loader.upsert_file_records` / `upsert_file_relationships`), and optionally detects and deletes floating/orphan nodes afterward (`delete_floating_nodes_if_found`).
- **`upsert_files_in_order`** — same as `upsert_files`, but accepts a **list** of S3 TSV folders (`tsv_folder_list_s3uri`) and loads them sequentially in the given order — useful when files must be loaded across multiple stages/dependencies.

### `validate_submission_against_db.py` — Validate submission files against database
Runs **database** validation (`Validator.validate_tsv_in_db`) against an existing graph database for a given `validation_mode` (`"New"`, `"Update"`, or `"Upsert"`), reporting whether records/relationships would validly load and projecting the resulting node/edge changes. If the submission files don't already have a `guid` column (`does_file_contain_uuid="no"`), the flow pauses to collect a `subgraph_value` and runs `add_uuid_to_files` first.

### `find_db_floating_nodes.py` — Find/delete floating (orphan) nodes
Two small helper flows built on `Loader`:
- **`find_floating_db_nodes_flow`** — streams all nodes with no path to a given root node label (default `"study"`) via `Loader.find_nodes_without_path_to_root` and writes them to a JSON file.
- **`delete_nodes_by_internal_id_flow`** — deletes nodes by their internal database ID (`Loader.delete_nodes_by_internal_id`).

These are used internally by `upsert_workflow.py` (optional cleanup step) rather than being deployed as standalone entrypoints.

### `precision_deletion_guid.py` — Precision Deletion Nodes
Deletes a specific set of nodes identified by a UUID/`guid` property value (`uuid_value_input`, given as a list of strings or an S3 URI to a JSON file of UUIDs). Supports a `dry_run` mode (default `True`) that reports what would be deleted without performing the deletion, backed by `Loader.delete_nodes_by_prop_value`.

### `delete_database_study_subgraph.py` — Delete Database Study Subgraph Flow
Deletes one or more entire rooted subgraphs (e.g. all data under one or more studies) by matching a root node label/property against a list of property values, using `Loader.wipe_rooted_subgraph` for each value.

### `find_anchored_traversals.py` — Find Anchored Traversals Flow
For each given "intermediate root" node value (e.g. a list of `participant` IDs) within a larger rooted subgraph (e.g. a `study`), renders an interactive HTML visualization of all descendant traversals (`Loader.viz_intermediate_anchored_traversals`) and uploads it to S3. Useful for inspecting what would be affected before deleting a node and its descendants.

### `wipe_database.py` — Wipe out Database Flow
Deletes **all** nodes, relationships, and indexes in the target database (`Loader.wipe_database` + `drop_all_indexes`). Destructive and irreversible — use with caution, typically only in non-production/test databases.

## Typical pipeline order
1. `validate_tsv_files` — local validation of submission TSVs.
2. `add_uuid_to_files` — generate `guid` values (if not already present).
3. `validate_submission_against_db` — check what would change in the target database.
4. `upsert_files` / `upsert_files_in_order` — load the data.
5. As needed: `find_db_floating_nodes`, `precision_deletion_guid`, `delete_database_study_subgraph`, `find_anchored_traversals`, or `wipe_database` for maintenance/cleanup.
