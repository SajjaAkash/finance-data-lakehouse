select
    cik,
    ticker,
    filing_url,
    source_system,
    cast(extracted_at as timestamp) as extracted_at
from {{ source('bronze', 'sec_submissions') }}
