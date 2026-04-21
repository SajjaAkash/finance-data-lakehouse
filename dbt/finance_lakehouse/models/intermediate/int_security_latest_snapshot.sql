with market as (
    select * from {{ ref('stg_market_data') }}
),
filings as (
    select
        ticker,
        max(extracted_at) as latest_filing_at
    from {{ ref('stg_sec_submissions') }}
    group by 1
)
select
    market.symbol as ticker,
    market.close_price,
    market.trade_volume,
    market.extracted_at as market_extracted_at,
    filings.latest_filing_at
from market
left join filings
    on market.symbol = filings.ticker
