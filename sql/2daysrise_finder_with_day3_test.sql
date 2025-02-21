-- THIS SCRIPT FINDS OCCURRENCES THAT MATCH MY 2 DAYS RISE CRITERIA, AND THEN ALSO
-- CALCULATES WHETHER DAY 3 WAS A RISE OR NOT BASED OFF THE HIGH OF DAY 3 VS CLOSE DAY 2
-- DAY = RISE IF +0.14%
-- NOTE THE TOTALS DO NOT REFLECT WINNING / LOSING TRADES
-- NOTE YOU MAY HAVE TO CHANGE DAY FOR DATE(TIME)

WITH price_changes AS (
    SELECT 
        symbol,
        day,
        close,
        high,
        LAG(close, 1) OVER (PARTITION BY symbol ORDER BY day) AS prev_close,
        LAG(close, 2) OVER (PARTITION BY symbol ORDER BY day) AS prev_prev_close,
        LAG(close, 3) OVER (PARTITION BY symbol ORDER BY day) AS prev_prev_prev_close,
        LEAD(high, 1) OVER (PARTITION BY symbol ORDER BY day) AS next_high
    FROM fx_15m_to_day
    WHERE tag = 'fx'
),
pattern_matches AS (
    SELECT 
        symbol,
        day AS day_2,  
        close AS day_2_close,
        next_high AS day_3_high,
        CASE 
            WHEN ((next_high - close) / close) * 100 >= 0.14 THEN 'Yes'
            ELSE 'No'
        END AS third_day_rise
    FROM price_changes
    WHERE 
        ((prev_prev_close - prev_prev_prev_close) / prev_prev_prev_close) * 100 BETWEEN -2 AND 0.05  
        AND ((prev_close - prev_prev_close) / prev_prev_close) * 100 >= 0.14  
        AND ((close - prev_close) / prev_close) * 100 >= 0.14
)
SELECT 
    *,
    (SELECT COUNT(*) FROM pattern_matches) AS total_occurrences,
    (SELECT COUNT(*) FROM pattern_matches WHERE third_day_rise = 'Yes') AS total_yes,
    (SELECT COUNT(*) FROM pattern_matches WHERE third_day_rise = 'No') AS total_no
FROM pattern_matches
ORDER BY symbol, day_2;
