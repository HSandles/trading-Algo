-- SCRIPT FOR GROUPING 96 15M CANDLES INTO WEEKLY ONES SET TO FX ATM
-- WHEN QUERYING THE CREATED TABLES - YOU NEED TO CHANGE TIME TO WEEK? AS TIME CLOUMN NO LONGER EXISTS


WITH weekly_assets AS (
    SELECT 
        symbol,
        tag,
        DATE_TRUNC('week', time + INTERVAL '1 day') - INTERVAL '1 day' AS week_start,  -- Adjust week to start from Sunday 5 PM EST
        MIN(time) AS first_time,  -- First timestamp of the week
        MAX(time) AS last_time,   -- Last timestamp of the week
        MAX(high) AS high,        -- Highest price of the week
        MIN(low) AS low           -- Lowest price of the week
    FROM candles
    WHERE tag = 'fx'
    GROUP BY symbol, tag, DATE_TRUNC('week', time + INTERVAL '1 day') - INTERVAL '1 day'
)
SELECT 
    w.symbol,
    w.tag,
    w.week_start,
    c_open.open_price AS open,  -- First open price of the week
    w.high,
    w.low,
    c_close.close AS close  -- Last close price of the week
FROM weekly_assets w
JOIN candles c_open 
    ON c_open.symbol = w.symbol 
    AND c_open.tag = w.tag 
    AND c_open.time = w.first_time
JOIN candles c_close 
    ON c_close.symbol = w.symbol 
    AND c_close.tag = w.tag 
    AND c_close.time = w.last_time
ORDER BY symbol, w.week_start DESC;
