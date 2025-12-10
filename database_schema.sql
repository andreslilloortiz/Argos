-- This file is part of Argos.

-- Copyright (C) 2025 Andrés Lillo Ortiz

-- This program is free software: you can redistribute it and/or modify
-- it under the terms of the GNU General Public License as published by
-- the Free Software Foundation, either version 3 of the License, or
-- (at your option) any later version.

-- This program is distributed in the hope that it will be useful,
-- but WITHOUT ANY WARRANTY; without even the implied warranty of
-- MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
-- GNU General Public License for more details.

-- You should have received a copy of the GNU General Public License
-- along with this program. If not, see <https://www.gnu.org/licenses/>.

CREATE DATABASE IF NOT EXISTS argos;

USE argos;

CREATE TABLE IF NOT EXISTS trades (
    event_time DateTime CODEC(Delta, ZSTD), -- Transaction date and time
    symbol String,                          -- Currency pair (e.g., BTCUSDT)
    price Float64,                          -- Execution price
    quantity Float64,                       -- Quantity of coins bought/sold
    quote_asset_volume Float64,             -- Total volume in quote currency (USD/USDT)
    is_buyer_maker UInt8                    -- 1 if the order maker was a buyer, 0 if a seller
) ENGINE = MergeTree()
ORDER BY (symbol, event_time) -- CRITICAL ordering key for speed/performance
PARTITION BY toYYYYMM(event_time); -- Partitions data by month for easier management