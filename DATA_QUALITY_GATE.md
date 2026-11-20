# Data Engineering Contribution: Pre-Publication Data Quality Gate

## 1. Overview

Implemented a reusable **Pre-Publication Data Quality Gate** for the Crypto Data Engineering pipeline to prevent invalid data batches from reaching downstream analytical layers.

The solution integrates with API ingestion and Silver processing while preserving existing pipeline behavior.

## 2. Problem Statement

The existing pipeline supported row-level validation and quarantine, but batch-level quality checks were needed before publishing accepted data.

Potential issues included:
- Empty or incomplete API responses replacing previously valid datasets.
- Invalid timestamps and numerical values.
- Duplicate or conflicting business keys.
- Excessive rejected records within a batch.
- Publication of batches that fail expected-date coverage requirements.

## 3. Implementation

**Reusable Validation Module**

Added `src/common/data_quality.py` to provide shared validation contracts, configurable rejection thresholds, and structured failure diagnostics.

**API Ingestion and Transformation**

- Validate API payloads before replacing Bronze data.
- Validate both API datasets before publishing accepted Silver data and SQL staging outputs.
- Preserve existing task dependencies and ingestion behavior.

**Silver Trade Processing**

- Validate incoming trade batches before accepted partition replacement.
- Detect invalid trade identifiers, missing required fields, and excessive rejection rates.
- Preserve existing deduplication and quarantine logic.

**Failure Handling**

- Raise `DataQualityError` when a batch fails validation.
- Prevent accepted-data publication when quality checks fail.
- Record structured failure diagnostics.
- Support successful recovery when corrected data is reprocessed.

## 4. Validation Rules

The implementation includes checks for:

- Empty batches and missing required fields.
- Missing expected symbols and explicitly required dates.
- Invalid timestamps and numerical values.
- Negative or otherwise invalid financial values.
- Duplicate and conflicting business keys.
- Excessive rejected records.

The rejection threshold is configurable through `DQ_MAX_REJECTION_RATE`, with a default value of `0.01` (1%).

## 5. Testing and Verification

- **135 automated tests passed**, including 41 Data Quality Gate unit and mocked-boundary tests.
- **1 test skipped** because its Airflow dependency was unavailable in the test environment.
- Ruff static analysis passed.
- Git whitespace validation passed.

Tests cover validation rules, failure handling, publication boundaries, and recovery scenarios.

## 6. Scope and Limitations

The quality gate prevents detected batch-quality failures from triggering accepted-data publication.

Cross-system transactional guarantees between Delta and JDBC are outside the scope of this contribution. End-to-end Airflow orchestration and live Delta/JDBC publication have not been verified as part of this repository's test run.

Expected-date coverage is enforced when explicitly supplied by the caller; the gate does not independently guarantee completeness of every historical source extract.

## 7. Files Changed

| File | Contribution |
|---|---|
| `src/common/data_quality.py` | Shared validation and diagnostics |
| `src/transform/api_daily.py` | API data validation |
| `src/transform/api_job.py` | Pre-publication validation |
| `src/transform/silver_aggtrades.py` | Trade field validation |
| `src/transform/silver_job.py` | Silver batch quality gate |
| `dags/api_daily_ingestion.py` | Bronze ingestion validation |
| `tests/test_data_quality.py` | Unit and mocked-boundary tests |
| `tests/test_silver_aggtrades.py` | Spark parsing regression tests |

## 8. Outcome

This contribution strengthens pipeline reliability by introducing reusable batch-level quality controls, preventing detected invalid batches from replacing accepted datasets, and improving failure diagnostics and recovery testing.