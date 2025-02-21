-- THIS SCRIPT FINDS OCCURRENCES THAT MATCH MY 2 WEEKS RISE CRITERIA 

WITH price_changes AS (
    SELECT 
        symbol,
        week_start,
        close,
        LAG(close, 1) OVER (PARTITION BY symbol ORDER BY week_start) AS prev_close,
        LAG(close, 2) OVER (PARTITION BY symbol ORDER BY week_start) AS prev_prev_close,
        LAG(close, 3) OVER (PARTITION BY symbol ORDER BY week_start) AS prev_prev_prev_close
    FROM fx_15m_to_week
    WHERE tag = 'fx'
)
SELECT symbol, week_start, close, prev_close, prev_prev_close, prev_prev_prev_close
FROM price_changes
WHERE 
    ((prev_prev_close - prev_prev_prev_close) / prev_prev_prev_close) * 100 BETWEEN -6 AND 0.15  -- Week 0: No net change or drop
    AND ((prev_close - prev_prev_close) / prev_prev_close) * 100 >= 0.3  -- Week 1: Rise of at least 0.3%
    AND ((close - prev_close) / prev_close) * 100 >= 0.3  -- Week 2: Rise of at least 0.3%
ORDER BY symbol, week_start;
