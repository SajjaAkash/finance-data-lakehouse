$project = "dbt/finance_lakehouse"
dbt seed --project-dir $project --profiles-dir $project --target dev
dbt build --project-dir $project --profiles-dir $project --target dev
