# Finance Data Lakehouse

AWS-based finance lakehouse starter project that ingests market and SEC data into Bronze, Silver, and Gold layers using Glue, dbt, Delta Lake, and Airflow.

## Architecture

- `sources`: Market data and SEC filings land in S3-backed Bronze tables.
- `processing`: AWS Glue PySpark jobs standardize and enrich data into Silver Delta tables.
- `transformation`: dbt builds warehouse-ready Gold marts with automated data quality tests.
- `orchestration`: Airflow coordinates ingestion, processing, and dbt runs.
- `platform`: Terraform provisions buckets, IAM roles, Glue jobs, and MWAA scaffolding.

## Repository Layout

- `src/finance_lakehouse/`: Shared Python utilities and Glue job code.
- `dags/`: Airflow DAGs for end-to-end orchestration.
- `dbt/finance_lakehouse/`: dbt project, models, seeds, and tests.
- `infrastructure/terraform/`: IaC for AWS resources.
- `.github/workflows/`: CI checks for Python and dbt assets.
- `tests/`: Unit tests for transformation helpers.

## Quick Start

1. Create a virtual environment and install the project:

   ```powershell
   python -m venv .venv
   .\.venv\Scripts\Activate.ps1
   pip install -e .[dev]
   ```

2. Copy environment variables:

   ```powershell
   Copy-Item .env.example .env
   ```

3. Validate Python assets:

   ```powershell
   pytest
   ```

4. Lint Terraform and dbt assets in CI or your preferred local tooling.

## Local Development Assumptions

- Python 3.11+
- AWS credentials available via your local profile or CI secret injection
- dbt adapter selected per warehouse target, currently configured for DuckDB locally and easily switchable for Redshift/Snowflake/Databricks

## GitHub Setup

After creating a GitHub repository:

```powershell
git init
git add .
git commit -m "Initial finance lakehouse scaffold"
git branch -M main
git remote add origin <your-github-repo-url>
git push -u origin main
```
