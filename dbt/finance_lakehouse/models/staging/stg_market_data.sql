select
    symbol,
    cast(close_price as double) as close_price,
    cast(trade_volume as bigint) as trade_volume,
    source_system,
    cast(extracted_at as timestamp) as extracted_at
from {{ source('bronze', 'market_data') }}
