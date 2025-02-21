
--THIS MIGHT NOT BE GOOD - NEEDS TESTING

WITH all_assets AS (
    SELECT 
        symbol,
        -- Define trading day starting at 10:01 PM UTC
        CASE 
            WHEN EXTRACT(DOW FROM time) = 5 AND EXTRACT(HOUR FROM time) >= 22 THEN DATE(time) + INTERVAL '3 days'  -- Friday 10:01 PM → Monday
            WHEN EXTRACT(HOUR FROM time) >= 22 THEN DATE(time) + INTERVAL '1 day'  -- Shift day forward after 10 PM
            ELSE DATE(time)
        END AS trading_day,
        
        MIN(time) AS first_time,  -- First timestamp of the trading day
        MAX(time) AS last_time,   -- Last timestamp of the trading day
        MAX(high) AS high,        -- Highest price of the trading day
        MIN(low) AS low           -- Lowest price of the trading day
    FROM candles
    WHERE symbol = 'EURUSD' 
      AND EXTRACT(DOW FROM time) BETWEEN 0 AND 5  -- Exclude Saturdays (6) & Sundays (0)
    GROUP BY symbol, trading_day
)
SELECT 
    d.symbol,
    d.trading_day,  -- Custom trading day
    c_open.open_price AS open,  -- First open price of the trading day
    d.high,
    d.low,
    c_close.close AS close  -- Last close price of the trading day
FROM all_assets d
JOIN candles c_open 
    ON c_open.symbol = d.symbol 
    AND CASE 
        WHEN EXTRACT(DOW FROM c_open.time) = 5 AND EXTRACT(HOUR FROM c_open.time) >= 22 THEN DATE(c_open.time) + INTERVAL '3 days'
        WHEN EXTRACT(HOUR FROM c_open.time) >= 22 THEN DATE(c_open.time) + INTERVAL '1 day'
        ELSE DATE(c_open.time)
    END = d.trading_day
    AND c_open.time = d.first_time
JOIN candles c_close 
    ON c_close.symbol = d.symbol 
    AND CASE 
        WHEN EXTRACT(DOW FROM c_close.time) = 5 AND EXTRACT(HOUR FROM c_close.time) >= 22 THEN DATE(c_close.time) + INTERVAL '3 days'
        WHEN EXTRACT(HOUR FROM c_close.time) >= 22 THEN DATE(c_close.time) + INTERVAL '1 day'
        ELSE DATE(c_close.time)
    END = d.trading_day
    AND c_close.time = d.last_time
ORDER BY d.symbol, d.trading_day DESC;
