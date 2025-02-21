-- SCRIPT FOR GROUPING 96 15M CANDLES INTO DAILY ONES SET TO FX ATM
-- WHEN QUERYING THE CREATED TABLES - YOU NEED TO CHANGE TIME TO HOUR AS TIME CLOUMN NO LONGER EXISTS



WITH all_assets AS (
    SELECT 
        symbol,
        tag,
        DATE_TRUNC('hour', time) AS hour,  -- Truncate to the hour
        MIN(time) AS first_time,  -- Earliest timestamp of the hour
        MAX(time) AS last_time,   -- Latest timestamp of the hour
        MAX(high) AS high,        -- Highest price in the hour
        MIN(low) AS low           -- Lowest price in the hour
    FROM candles
    WHERE tag = 'fx'
    GROUP BY symbol, tag, DATE_TRUNC('hour', time)
)
SELECT 
    d.symbol,
    d.tag,
    d.hour,
    c_open.open_price AS open,  -- First open price of the hour
    d.high,
    d.low,
    c_close.close AS close  -- Last close price of the hour
FROM all_assets d
JOIN candles c_open 
    ON c_open.symbol = d.symbol 
    AND c_open.tag = d.tag 
    AND DATE_TRUNC('hour', c_open.time) = d.hour 
    AND c_open.time = d.first_time
JOIN candles c_close 
    ON c_close.symbol = d.symbol 
    AND c_close.tag = d.tag 
    AND DATE_TRUNC('hour', c_close.time) = d.hour 
    AND c_close.time = d.last_time
ORDER BY symbol, d.hour DESC;
