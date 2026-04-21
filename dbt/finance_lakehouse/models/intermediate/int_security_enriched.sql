select
    g.ticker,
    s.company_name,
    s.sector,
    s.primary_exchange,
    s.asset_class,
    s.country_code,
    s.currency_code,
    s.region,
    s.industry_group,
    g.close_price,
    g.trade_volume,
    g.market_extracted_at,
    g.latest_filing_at,
    g.coverage_status
from {{ ref('gold_security_master') }} as g
left join {{ ref('security_universe') }} as s
    on g.ticker = s.ticker
