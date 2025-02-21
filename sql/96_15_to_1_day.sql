-- SCRIPT FOR GROUPING 96 15M CANDLES INTO DAILY ONES SET TO FX ATM
-- WHEN QUERYING THE CREATED TABLES - YOU NEED TO CHANGE TIME TO DAY AS TIME CLOUMN NO LONGER EXISTS

WITH all_assets AS (
    SELECT 
        symbol,
        tag,
        DATE(time) AS day,
        MIN(time) AS first_time,  -- Earliest timestamp of the day
        MAX(time) AS last_time,   -- Latest timestamp of the day
        MAX(high) AS high,        -- Highest price of the day
        MIN(low) AS low           -- Lowest price of the day
    FROM candles
    WHERE tag = 'fx'
    GROUP BY symbol, tag, DATE(time)
)
SELECT 
    d.symbol,
    d.tag,
    d.day,
    c_open.open_price AS open,  -- First open price of the day
    d.high,
    d.low,
    c_close.close AS close  -- Last close price of the day
FROM all_assets d
JOIN candles c_open 
    ON c_open.symbol = d.symbol 
    AND c_open.tag = d.tag 
    AND DATE(c_open.time) = d.day 
    AND c_open.time = d.first_time
JOIN candles c_close 
    ON c_close.symbol = d.symbol 
    AND c_close.tag = d.tag 
    AND DATE(c_close.time) = d.day 
    AND c_close.time = d.last_time
ORDER BY symbol, d.day DESC;


