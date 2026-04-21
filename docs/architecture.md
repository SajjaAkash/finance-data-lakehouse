# Architecture Notes

## End-to-End Data Flow

1. Market data and SEC submission metadata land in Bronze S3 prefixes partitioned by ingestion date.
2. Glue PySpark Bronze jobs standardize raw payload shapes and register downstream table locations.
3. Glue PySpark Silver jobs apply normalization, deduplication, and Delta Lake merge semantics.
4. dbt models transform curated Silver datasets into analytics-ready Gold marts.
5. MWAA orchestrates ingestion, Silver processing, dbt seed, dbt build, and summary publication.

## Medallion Layering

### Bronze

- Immutable, ingestion-date-partitioned landing paths in S3.
- Market dataset and SEC submissions dataset tracked independently.
- Write mode optimized for append-only raw extracts.

### Silver

- Conformed Delta Lake datasets for `market_data` and `sec_submissions`.
- Merge keys defined explicitly for idempotent upserts.
- Table registration SQL and write options captured in code for deployment parity.

### Gold

- `gold_security_master` provides analytics-ready security coverage state.
- `gold_security_coverage_summary` aggregates sector and region coverage metrics.
- dbt tests provide a strong semantic validation layer over curated outputs.

## Operational Components

- `Glue`: scalable ingestion and curation workloads.
- `MWAA`: orchestration and operational scheduling.
- `Terraform`: reproducible platform provisioning.
- `GitHub Actions`: repository-level CI guardrails.

## Design Choices

- Business semantics live in dbt instead of being embedded in Glue jobs.
- Glue jobs own extract, conformance, and Delta-oriented persistence behavior.
- Airflow coordinates across systems rather than holding heavy transformation logic.
- Terraform manages platform resources so the repo is reviewable as a full-stack data platform project.
