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