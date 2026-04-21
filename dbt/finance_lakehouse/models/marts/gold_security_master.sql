select
    ticker,
    close_price,
    trade_volume,
    market_extracted_at,
    latest_filing_at,
    case
        when latest_filing_at is null then 'market_only'
        else 'market_and_sec'
    end as coverage_status
from {{ ref('int_security_latest_snapshot') }}
