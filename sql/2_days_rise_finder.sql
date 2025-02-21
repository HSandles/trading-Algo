-- FROM IS WHAT THIS SCRIPT WILL BE READING 
-- THIS SCRIPT FINDS OCCURRENCES THAT MATCH MY 2 DAYS RISE CRITERIA 

WITH price_changes AS (
    SELECT 
        symbol,
        day,
        close,
        LAG(close, 1) OVER (PARTITION BY symbol ORDER BY day) AS prev_close,
        LAG(close, 2) OVER (PARTITION BY symbol ORDER BY day) AS prev_prev_close,
        LAG(close, 3) OVER (PARTITION BY symbol ORDER BY day) AS prev_prev_prev_close
    FROM fx_15m_to_day
	WHERE symbol = 'EURUSD'
)
SELECT symbol, day, close, prev_close, prev_prev_close, prev_prev_prev_close
FROM price_changes
WHERE 
    ((prev_prev_close - prev_prev_prev_close) / prev_prev_prev_close) * 100 BETWEEN -2 AND 0.05  -- Day 0: No net change or drop
    AND ((prev_close - prev_prev_close) / prev_prev_close) * 100 >= 0.14  -- Day 1: Rise by at least 0.15%
    AND ((close - prev_close) / prev_close) * 100 >= 0.14  -- Day 2: Rise by at least 0.15%
ORDER BY symbol, day;
