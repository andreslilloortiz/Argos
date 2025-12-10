CREATE DATABASE IF NOT EXISTS argos;

USE argos;

CREATE TABLE
    IF NOT EXISTS trades (
        event_time DateTime CODEC (Delta, ZSTD), -- Transaction date and time
        symbol String, -- Currency pair (e.g., BTCUSDT)
        price Float64, -- Execution price
        quantity Float64, -- Quantity of coins bought/sold
        quote_asset_volume Float64, -- Total volume in quote currency (USD/USDT)
        is_buyer_maker UInt8 -- 1 if the order maker was a buyer, 0 if a seller
    ) ENGINE = MergeTree ()
ORDER BY
    (symbol, event_time) -- CRITICAL ordering key for speed/performance
PARTITION BY
    toYYYYMM (event_time);

-- Partitions data by month for easier management