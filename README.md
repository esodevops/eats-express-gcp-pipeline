# Eats Express GCP Pipeline

An Apache Beam project for cleaning food-order CSV data and separating orders by delivery status. A Jupyter notebook runs the workflow locally and writes text files. A Python script contains the BigQuery loading workflow and accepts Beam runner options for cloud execution.

**Current state:** the local notebook pipeline has been verified against the included CSV. The BigQuery script requires the corrections listed under [Known limitations](#known-limitations) before cloud execution. Airflow dependencies are installed, but this repository does not yet include an Airflow DAG or infrastructure deployment configuration.

## Contents

- [Processing workflow](#processing-workflow)
- [Repository structure](#repository-structure)
- [Source data](#source-data)
- [Requirements](#requirements)
- [Local setup](#local-setup)
- [Run the notebook](#run-the-notebook)
- [BigQuery pipeline](#bigquery-pipeline)
- [Validation](#validation)
- [Troubleshooting](#troubleshooting)
- [Known limitations](#known-limitations)
- [Version control](#version-control)

## Processing workflow

```text
food_daily.csv
    |
    +-- Skip the header
    +-- Remove a trailing colon from the items field
    +-- Convert all fields to lowercase
    +-- Remove characters other than letters, digits, and spaces
    |
    +-- Status = delivered ------> Local delivered text output
    |
    +-- Status = not delivered --> Local undelivered text output

Python script extension:
    Cleaned rows --> Append count --> Map fields --> BigQuery writes
```

Both workflows match delivery status after trimming surrounding whitespace. `On Hold` and `Cancelled` orders are excluded from both output branches. The script appends a count field named `new_column`; the notebook does not append this field.

The cleanup currently applies to every field. For example, `20.50` becomes `2050`, and punctuation in dates and times is removed. See the limitations before using the transformed data for analysis.

## Repository structure

```text
eats-express-gcp-pipeline/
├── data/
│   ├── food_daily.csv             # Included source data
│   └── output/processed/          # Generated notebook output; ignored by Git
├── notebooks/
│   └── eats-express.ipynb         # Data inspection and local Beam pipeline
├── src/
│   └── beam.py                   # Command-line pipeline with BigQuery sinks
├── .gitignore
├── README.md
└── requirements.txt              # Exact direct dependency versions
```

## Source data

The CSV contains a header followed by 11 fields. The pipeline uses column positions, so their order matters.

| Position (zero-based) | Source column | Script destination field |
| --- | --- | --- |
| 0 | `Customer_id` | `Customer_id` |
| 1 | `date` | `date` |
| 2 | `time` | `timestamp` |
| 3 | `order_id` | `order_id` |
| 4 | `items` | `items` |
| 5 | `amount` | `amount` |
| 6 | `mode` | `mode` |
| 7 | `restaurnt` | `restaurant` |
| 8 | `Status` | `status` |
| 9 | `ratings` | `ratings` |
| 10 | `feedback` | `feedback` |

The script adds `new_column` after these fields with the value ` 1` as text. Its declared schema uses `INTEGER` for `amount` and `STRING` for the other fields; the row mapper currently returns strings for every field.

The included sample has **891 orders**:

| Source status | Rows | Current routing |
| --- | ---: | --- |
| `Delivered` | 869 | Delivered branch |
| `Not delivered` | 7 | Undelivered branch |
| `On Hold` | 10 | Excluded |
| `Cancelled` | 5 | Excluded |
| **Total** | **891** | **876 rows in output branches** |

## Requirements

Use **Python 3.12** for the pinned dependency set. The versions are maintained in `requirements.txt`:

| Package | Version | Purpose |
| --- | --- | --- |
| `pandas` | 2.1.4 | Notebook data inspection |
| `apache-beam[gcp]` | 2.70.0 | Pipeline transforms and GCP integrations |
| `apache-airflow` | 2.10.5 | Dependency for planned orchestration |
| `google-cloud-storage` | 2.19.0 | Cloud Storage client |
| `google-cloud-bigquery` | 3.20.1 | BigQuery dataset management |
| `apache-airflow-providers-google` | 12.0.0 | Airflow GCP integrations |
| `apache-airflow-providers-apache-beam` | 6.0.0 | Airflow Beam integrations |

Beam 2.70.0 provides a Python 3.12 macOS ARM64 wheel, avoiding the Beam source-build failure encountered with 2.61.0 on Apple Silicon. The requirements pin direct dependencies; they are not a complete lockfile for transitive dependencies.

## Local setup

Run these commands from the repository root. If an existing virtual environment is active, run `deactivate` first. Create a fresh environment when switching Python versions rather than recreating one over an older environment.

```bash
python3.12 --version
python3.12 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install --only-binary=apache-beam -r requirements.txt
python -m pip check
```

If you already have a working Python 3.12 environment in `venv/`, activate it with `source venv/bin/activate` instead of creating `.venv/`.

Use `python -m pip` to ensure package installation uses the active Python interpreter.

### Notebook support

Install the notebook tools separately; they are not listed in the pipeline requirements:

```bash
python -m pip install jupyterlab ipykernel
python -m ipykernel install --user --name eats-express --display-name "Eats Express (Python 3.12)"
```

Select that kernel in JupyterLab or VS Code before running the notebook.

## Run the notebook

The notebook uses paths relative to `notebooks/`. Start JupyterLab from that directory:

```bash
cd notebooks
python -m jupyterlab
```

Open `eats-express.ipynb` and run the cells in order:

1. Import the dependencies.
2. Load the source CSV and inspect columns, null values, and data types.
3. Define the row-cleaning functions.
4. Run the Beam pipeline to clean, filter, and write the orders.

Expected output prefixes, relative to the repository root:

```text
data/output/processed/delivered-*
data/output/processed/undelivered-*
```

Beam appends shard suffixes such as `-00000-of-00001`. The text files contain comma-separated rows without a header. Shard counts and row ordering are not guaranteed.

For the included sample, expect **869 delivered rows** and **7 undelivered rows** across the respective shards. Local execution does not require Google Cloud credentials.

After editing a helper function, rerun its definition cell before the pipeline cell. Restarting the kernel and running all cells also ensures the pipeline uses the saved definitions.

## BigQuery pipeline

### Implementation status

`src/beam.py` exposes an `--input` argument and forwards remaining arguments to `PipelineOptions`. Its `main()` function performs dataset setup before starting Beam. Importing the module does not submit a pipeline.

**Resolve the cloud-specific issues in [Known limitations](#known-limitations) before running this script.** Cloud loading has not been verified, and the commands below describe its intended execution interface.

### Configuration

The script currently hardcodes these values:

| Constant | Current value |
| --- | --- |
| `DELIVERED_TABLE_SPEC` | `eats-express-512014:eats_express_dataset.delivered_orders` |
| `OTHERS_TABLE_SPEC` | `eats-express-512014:eats_express_dataset.other_status_orders` |
| `DATASET_ID` | `eats-express-512014:eats_express_dataset` |
| Dataset location on creation | `US` |

Replace the project and destination values for your environment. Passing `--project` configures Beam; it does **not** replace these hardcoded table or dataset values.

The intended table layout separates delivered orders from not-delivered orders. Currently both sinks reference `DELIVERED_TABLE_SPEC`, and `OTHERS_TABLE_SPEC` is unused.

### Cloud prerequisites

For cloud execution, prepare:

- A Google Cloud project with billing and the services required for the chosen runner enabled.
- Application Default Credentials for the submitting process.
- Permissions to inspect or create the dataset and write to its tables.
- For Dataflow, a worker service account with access to the input, temporary storage, and BigQuery destinations, plus permission for the submitter to use that account.
- For GCS input or Dataflow temporary files, accessible Cloud Storage buckets with locations compatible with the destination dataset and job.

If using the Google Cloud CLI locally, configure Application Default Credentials with:

```bash
gcloud auth application-default login
```

Store credentials outside the repository. The script does not automatically load `.env` files.

### DirectRunner interface

After correcting the cloud issues, run from the repository root:

```bash
python src/beam.py \
  --input data/food_daily.csv \
  --runner DirectRunner \
  --project YOUR_PROJECT_ID \
  --temp_location gs://YOUR_BUCKET/tmp
```

DirectRunner executes the transforms locally, but this script still accesses BigQuery and can use GCS for file-load staging. Use the notebook for a local text-output workflow.

### DataflowRunner interface

After correcting the cloud issues and configuring destinations, an example submission is:

```bash
python src/beam.py \
  --input gs://YOUR_BUCKET/input/food_daily.csv \
  --runner DataflowRunner \
  --project YOUR_PROJECT_ID \
  --region YOUR_DATAFLOW_REGION \
  --staging_location gs://YOUR_BUCKET/staging \
  --temp_location gs://YOUR_BUCKET/tmp \
  --job_name eats-express-orders \
  --save_main_session
```

Replace all placeholders and choose a job name suitable for the submission. A local path on the submitting Mac is not an appropriate input path for remote workers. No Dataflow job has been submitted as part of local validation.

Both BigQuery sinks use `CREATE_IF_NEEDED` and `WRITE_APPEND`. Reprocessing the same source can append duplicate orders; there is no deduplication or merge step.

## Validation

Validation performed on the current local workflow:

- Python script and notebook code cells parse successfully.
- Installed direct dependency versions match `requirements.txt`.
- `python -m pip check` reports no broken requirements.
- The notebook pipeline completes against the included CSV, writing 869 delivered and 7 undelivered rows to temporary output files.

There is no committed automated test suite. BigQuery table creation, loading, partitioning, Dataflow execution, and Airflow orchestration have not been validated.

To check your current environment:

```bash
python --version
python -m pip check
python -c "import pandas, apache_beam; print('Data-processing imports passed')"
```

To count the generated notebook output rows, run from the repository root:

```bash
python - <<'PY'
from pathlib import Path

output_dir = Path("data/output/processed")
for status in ("delivered", "undelivered"):
    files = list(output_dir.glob(f"{status}-*"))
    count = sum(len(path.read_text().splitlines()) for path in files)
    print(f"{status}: {count} rows across {len(files)} files")
PY
```

These counts assume output files come from the current sample run, without stale shards from earlier runs.





