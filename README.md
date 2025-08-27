# Age-Normed MRIQC Dashboard

## Overview
This dashboard displays MRI quality metrics with age-specific thresholds. It reduces false rejections in child and aging cohorts.

## Setup
Install dependencies using:

```bash
pip install -r requirements.txt
```

Run the development server with:

```bash
uvicorn app.main:app --reload
```

## Test Data
A sample dataset is provided in the `data` folder to help you get started with input formats. You can replace it with your own data.

## Endpoints
- GET `/` : Basic project info.
- GET `/api/health` : Health check with service metadata.
- POST `/api/upload` : Upload an MRIQC JSON or ZIP; returns file info and validation.
- POST `/api/process` : Process a previously uploaded file and compute metrics.
- GET `/api/dashboard/summary` : Study-wide QC summary with age-normed thresholds.
- GET `/api/subjects` : List subjects with QC status; supports filters.
- POST `/api/export/csv` : Export summary CSV; POST `/api/export/pdf` for a PDF report.

Example:

```bash
curl -s http://localhost:8000/api/health

curl -s -X POST http://localhost:8000/api/process \
  -H 'Content-Type: application/json' \
  -d '{"sample_id": 1, "value": 2.0}'

# Upload MRIQC JSON
curl -s -X POST http://localhost:8000/api/upload \
  -F 'file=@/path/to/mriqc.json' | jq .

# Dashboard summary
curl -s http://localhost:8000/api/dashboard/summary | jq '.summary, .age_groups[0]'
```

## Contributor Quickstart

```bash
# 1) Create and activate a virtualenv
python3 -m venv .venv && source .venv/bin/activate

# 2) Install dependencies (prefer editable + dev extras)
pip install -U pip
pip install -e .[dev] || pip install -r requirements.txt

# 3) Run the API locally
uvicorn app.main:app --reload

# 4) Run tests and checks
pytest -q
black . && isort . && flake8 app tests && mypy app

# 5) Commit using Conventional Commits
# e.g., feat: age-binned thresholds in summary API
```

## API Quick Reference

- POST `/api/upload`: Upload MRIQC JSON or ZIP.
- POST `/api/process`: Process and compute metrics.
- GET `/api/dashboard/summary`: Aggregate QC summary with age-normed thresholds.
- GET `/api/subjects`: List/filter subjects and QC status.
- POST `/api/export/csv` | `/api/export/pdf`: Export summaries.
