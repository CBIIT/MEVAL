---
layout: default
title: Validator Workflows
permalink: /validator/workflows/
---
# Validator Workflows

MEVAL repo contains ready-to-deploy Prefect workflows. A handle of ready-to-load Prefect flows below (in `workflows/prefect/`) use `meval.validator.Validator` — either by instantiating it against an `MDFReader` model, or by calling its `@staticmethod`/`@classmethod` helpers directly — to validate or prepare TSV submission files.

## Contents
- [`validate_tsv_files.py` — Validate TSV Files](#validate_tsv_filespy--validate-tsv-files)
- [`add_uuid_to_files.py` — Add UUID to a set of TSV files](#add_uuid_to_filespy--add-uuid-to-a-set-of-tsv-files)
- [`validate_submission_against_db.py` — Validate submission files against database](#validate_submission_against_dbpy--validate-submission-files-against-database)

## `validate_tsv_files.py` — Validate TSV Files
Downloads model files, creates a `Validator(mdf=model_mdf)` instance, and runs the full **local** validation pipeline against all TSV files found under an S3 folder (via the static `Validator.find_tsv_files`):
- `validator.validate_tsv_files_format(...)` — TSV format check (always run).
- `validator.validate_tsv_records(...)` — per-record property validation (optional, via `validation_items`).
- `validator.validate_tsv_rels(...)` — cross-file relationship/linkage validation (optional).
- `validator.validate_tsv_uniq_entry(...)` — duplicate key-value check (optional).

Validation results/summaries are uploaded back to S3. All files for a study/program should be included in one run since partial file sets can miss cross-file relationship issues.

## `add_uuid_to_files.py` — Add UUID to a set of TSV files
Uses `Validator.find_tsv_files` to locate TSV files downloaded from S3, then calls the static `Validator.add_uuid_to_tsv_file(file_path, project_name, mdf, output_file_path, uuid_column, delimiter, subgraph_value)` for each file to generate deterministic UUID5 values for records and relationship columns, writing `*_added.tsv` output files that are uploaded back to S3.

## `validate_submission_against_db.py` — Validate submission files against database
Creates a `Validator(mdf=mdf_instance)` instance (`val_instance`) and uses it for **database** validation:
- `val_instance.validate_tsv_in_db(driver, tsv_file_path, tsv_id_set, mdf_instance, id_prop_name, delimiter, validation_mode)` — run once per file, checking each row against the target database for the selected `validation_mode` (`"New"`, `"Update"`, or `"Upsert"`) and returning passed/failed row lists plus a validation summary with projected node/edge changes.

If the submission files don't already have a `guid` column (`does_file_contain_uuid="no"`), the flow first pauses to collect a `subgraph_value` and calls the `add_uuid_to_files` flow before validating.

