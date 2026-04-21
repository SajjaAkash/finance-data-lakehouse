# Architecture Notes

## Data Flow

1. Source extracts land in Bronze S3 paths partitioned by ingestion date.
2. Glue transforms normalize raw payloads into conformed Silver records stored as Delta-formatted tables.
3. dbt builds analytics-ready Gold marts from curated upstream datasets.
4. Airflow orchestrates the dependency graph and provides operational observability.

## Core Design Choices

- Medallion layering keeps raw ingestion isolated from business logic.
- dbt owns semantic transformations and tests.
- Glue is reserved for scalable extract and standardization work.
- Airflow coordinates cross-system dependencies instead of embedding orchestration inside jobs.

## Next Enhancements

- Replace manifest stubs with real API and SEC fetch clients.
- Add Delta table registration in Glue Catalog or Lake Formation.
- Parameterize Airflow tasks to trigger true Glue jobs and dbt execution.
- Add warehouse-specific dbt target profiles for production.
