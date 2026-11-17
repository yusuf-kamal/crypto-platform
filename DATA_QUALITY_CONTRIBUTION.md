# Individual contribution: pre-publication data quality gate

## Original problem

Row-level quarantine already existed, but a successful transformation did not
prove that its batch was safe to publish. An empty API response could replace
valid Silver and staging datasets. Malformed API timestamps/numbers could raise
before quarantine, and duplicate timestamps could disappear during parsing.
Silver also published accepted trades regardless of the batch rejection rate.

## Solution and boundaries

1. API ingestion validates the response envelope and row/batch contract before
   overwriting its Bronze objects. All configured CoinGecko responses are
   fetched and validated before that task writes any of them. Fear & Greed is
   a separate task, as before.
2. API transformation repeats validation for replays. **Both** API datasets must
   pass before either accepted Silver dataset or SQL staging table is written.
   Both accepted Spark frames are constructed before publication starts.
3. Silver trades retain existing parsing, quarantine and deduplication. Before
   publishing the accepted partition, the gate checks counts, duplicate loss,
   rejection rate and explicitly supplied expected dates. Only the small
   distinct-date summary is collected to the driver.
4. `DataQualityError` fails the task, preventing its normal manifest/asset
   publication. The structured `data_quality_failed` event includes reasons and
   counts. An immutable JSON report is attempted under
   `<bucket>/_quarantine/_quality_failures/<uuid>.json`: Bronze for ingestion,
   Silver for transformations. A diagnostic-storage failure does not mask the
   original exception; the log still contains the report.

The reusable `src/common/data_quality.py` has no new dependencies. It provides
summary/row contracts, payload checks, configuration and diagnostic handling.
API-specific numeric and timestamp parsing stays in `api_daily.py`.

## Policy

- Empty input, no valid rows, missing symbols/required fields, duplicate or
  conflicting business keys, and missing expected dates block publication.
- Malformed timestamps, NaN/infinity, invalid numeric types, nonpositive prices,
  negative market cap/volume and values beyond warehouse decimal capacity are
  rejected. Invalid trade fields and negative trade IDs are quarantined.
- Nullable CoinGecko market cap and volume remain valid. Zero cap/volume is valid.
- `DQ_MAX_REJECTION_RATE` is read from the **task process environment** at runtime;
  default `0.01`. Set `0` to reject any bad row. The boundary is inclusive:
  exactly 1% passes the rate check; other checks still apply. Invalid settings
  fail closed. Existing Compose does not forward this variable, so editing
  `.env` alone does not configure it; no Compose change is included here.
- The final day's unfinished CoinGecko intraday points remain quarantined and
  excluded from the bad-data budget. Malformed numeric values are never exempt.
- Coverage is enforced only against an explicitly supplied `expected_dates`
  contract. The jobs do not infer that every monthly file has trades on every
  calendar day, or that API history extends through the Bronze logical date.
  This preserves legitimate partial historical datasets and delayed retries.
  Callers currently supply no expected date set, so automatic completeness
  detection is limited to nonempty batches and required symbols. Timestamp
  parsing and the existing Silver file-month membership check remain enforced.

Existing row quarantine paths are retained. Failed API batches use JSON
diagnostics instead of forcing uncastable values into a typed Delta quarantine
table; their original data remains in Bronze. Failed ingestion reports retain
the rejected payload. Tolerated API rejects still use existing row quarantine;
uncastable quarantine cells become null and raw values remain in Bronze.

## Files changed

- Added `src/common/data_quality.py`: reusable contracts and diagnostics.
- Updated `src/transform/api_daily.py`: robust parsing and API contracts.
- Updated `src/transform/api_job.py`: validate both datasets before publication.
- Updated `dags/api_daily_ingestion.py`: validate before Bronze replacement.
- Updated `src/transform/silver_aggtrades.py`: required-field and ID checks.
- Updated `src/transform/silver_job.py`: gate before accepted partition replacement.
- Added `tests/test_data_quality.py`: unit tests and mocked writer regressions.
- Extended `tests/test_silver_aggtrades.py`: real Spark parsing regressions.

## Tests

From the repository root, with the project's existing Python dependencies:

```text
python -B -m unittest discover -s tests -p test_data_quality.py -v
python -m pytest tests/test_data_quality.py tests/test_api_daily.py tests/test_api_sources.py tests/test_silver_aggtrades.py tests/test_lake.py tests/test_dag_integrity.py -q
```

The unittest suite uses installed PySpark types but never starts a JVM, Airflow,
S3, Docker or SQL Server. It exercises actual API job control flow and the Silver
publication helper with mocked external boundaries. These are **unit and mocked
boundary tests**, not end-to-end integration tests.

See the migration verification below for results from the Git repository.

## Safe failure/recovery demonstration

Run the unittest command above, or just the focused demonstration:

```text
python -B -m unittest discover -s tests -p test_data_quality.py -k failure_then_corrected_payload_recovers -v
```

The test seeds fake storage with "previous good data", supplies an empty API
payload, asserts `DataQualityError`, verifies no accepted-data writer ran and
checks a diagnostic was saved. It then restores a valid payload, reruns the same
job and verifies both datasets publish. Companion tests demonstrate failure in
the second API dataset, invalid numbers, missing symbols and duplicates.

For a future integration demo, use a disposable project environment and record
Silver versions and staging counts before/after the same injected failures.
Correct the source payload and retry the failed task, then verify downstream
publication. Do not remove production manifests or alter production datasets.

## Remaining limits

This gate prevents **quality failures** from replacing accepted data. It does
not provide a distributed transaction across Delta and JDBC: an infrastructure
failure after publication starts can still leave a partial handoff. That is the
separate reliability recommendation. Existing manifest skips are unchanged;
previously processed historical files are not automatically revalidated. Daily
coverage is not proof of complete intraday trade coverage. Duplicate detection
is within each incoming batch, not a new cross-month global uniqueness index.
Checking repeated IDs across accepted and rejected trades adds a distributed
aggregation; its runtime overhead on the full monthly files has not been measured.

## Migration verification (2026-11-17)

Migrated from the downloaded `crypto-platform-main/crypto-platform-main`
workspace into `crypto-platform-git`, using the destination Git commit
`5c4a234` as the baseline. The working tree was clean on
`feature/data-quality-gate`; local `main` and the cached `origin/main` also
pointed at that commit. Remote freshness was not checked by fetching.

The six existing files were compared individually and updated with a checked
patch containing the reviewed feature hunks. The two new Python files retain
the old implementation; this document has been updated for the destination.
Existing function signatures, lake paths and format selection, partition
replacement predicates, API logical-date resolution, metrics, DAG schedule,
assets and downstream task wiring are preserved. No dependency, Compose,
warehouse schema or unrelated runtime file changes are included.

The contribution's lazy `boto3` import in `silver_job.run` allows the publication
helper tests to run with the existing Python CI dependencies, which do not
install boto3. The new DAG imports depend only on the standard library and
existing project modules; they do not require PySpark at DAG parse time.

Verification in this repository:

- **135 passed, 1 skipped** for the full `tests` suite, including the 41
  quality-gate unit/mocked-boundary tests and all eight added real Spark
  parsing cases. The skipped module is `test_dag_integrity.py`, because Airflow
  is not installed in this Windows environment.
- `ruff check . --no-cache`: all checks passed.
- `git diff --check`: passed. The final scope is exactly six modified files
  and the three new contribution files, with nothing staged. The diff and new
  files were reviewed for accidental edits, credentials and generated files.
- All 99 tracked destination files were compared against the old project.
  After applying the feature hunks, their content matches the old project
  (ignoring line endings); there were no additional code conflicts to resolve.

The isolated test environment uses Python 3.11.9, PySpark 3.5.1, pytest 8.3.4,
requests 2.32.3 and Java 17. Initial host settings pointed at stale Java/Spark
locations and global PySpark 4.2.0. Dependencies were installed only in `.venv/`;
the test process selected the existing Java 17 installation and the venv's
PySpark. The first full run had three failures in unchanged streaming tests
because Python converted UTC timestamps to the Windows local time zone.
Setting `TZ=UTC` for the test process resolved all three without source edits.

Reproduce the successful run in PowerShell from this repository root:

```powershell
$env:JAVA_HOME = 'C:\Program Files\Eclipse Adoptium\jdk-17.0.20.101-hotspot'
Remove-Item Env:SPARK_HOME -ErrorAction SilentlyContinue
$env:PYSPARK_PYTHON = (Resolve-Path '.\.venv\Scripts\python.exe').Path
$env:SPARK_LOCAL_IP = '127.0.0.1'
$env:TZ = 'UTC'
.\.venv\Scripts\python.exe -B -m pytest tests -q -o cache_dir=.venv/pytest-cache
.\.venv\Scripts\python.exe -m ruff check . --no-cache
git diff --check
```

These environment assignments affect only that PowerShell process and its
children; no persistent system settings were changed during verification.

Write-boundary review:

- Bronze: all CoinGecko responses pass before its first write, but the five
  objects and independent Fear & Greed task are not one atomic transaction.
  Network failure or a sibling task failure can leave a partial Bronze snapshot.
- Silver/staging: quality failures block accepted writers. Successful writes
  are still sequential; an injected JDBC failure confirms Silver can advance
  while staging remains old. Delta atomicity is per table, not across systems.
- Quarantine: diagnostic writes are allowed on failure. Row-quarantine overwrites
  retain their existing behavior and can replace earlier diagnostics. A new API
  quarantine-write failure prevents subsequent accepted writes; JSON diagnostic
  persistence is best effort and logs remain the fallback.
- Partial histories: acceptance is backward-compatible, but the original overwrite
  scope is unchanged. Replaying a partial extract over a larger existing snapshot
  may remove data outside that extract. The gate does not certify completeness
  without an explicit source contract.

Actual Airflow imports and Delta/JDBC publication require their project runtime.
Local tests do not modify production tables, Docker volumes or existing
containers. The isolated `.venv/` is ignored and is not part of the contribution.
No commit or push is part of this migration.
