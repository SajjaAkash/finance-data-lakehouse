select
    sector,
    region,
    coverage_status,
    count(*) as security_count,
    sum(trade_volume) as total_trade_volume,
    avg(close_price) as average_close_price,
    max(market_extracted_at) as latest_market_extract_at
from {{ ref('int_security_enriched') }}
group by 1, 2, 3
