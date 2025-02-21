-- THIS SCRIPT FINDS OCCURRENCES THAT MATCH MY 2 WEEKS RISE CRITERIA, AND THEN ALSO
-- CALCULATES WHETHER WEEK 3 WAS A DROP OR NOT BASED OFF THE LOW OF WEEK 3 VS CLOSE WEEK 2
-- DAY = RISE IF +0.3%


WITH price_changes AS (
    SELECT 
        symbol,
        week_start,
        open,
        close,
        low,
        LAG(close, 1) OVER (PARTITION BY symbol ORDER BY week_start) AS prev_close,
        LAG(close, 2) OVER (PARTITION BY symbol ORDER BY week_start) AS prev_prev_close,
        LAG(close, 3) OVER (PARTITION BY symbol ORDER BY week_start) AS prev_prev_prev_close,
        LEAD(open, 1) OVER (PARTITION BY symbol ORDER BY week_start) AS next_open, -- Week 3's Open
        LEAD(low, 1) OVER (PARTITION BY symbol ORDER BY week_start) AS next_low   -- Week 3's Low
    FROM fx_15m_to_week
    WHERE tag = 'fx'
),
pattern_matches AS (
    SELECT 
        symbol,
        week_start AS week_2,  
        close AS week_2_close,
        next_open AS week_3_open,
        next_low AS week_3_low,
        CASE 
            WHEN ((next_low - LEAST(close, next_open)) / LEAST(close, next_open)) * 100 <= -0.3 
            THEN 'Yes'  -- Week 3 dropped by at least 0.3% (comparing low)
            ELSE 'No'
        END AS third_week_drop
    FROM price_changes
    WHERE 
        ((prev_prev_close - prev_prev_prev_close) / prev_prev_prev_close) * 100 BETWEEN -6 AND 0.15  -- Week 0: No net change or drop  
        AND ((prev_close - prev_prev_close) / prev_prev_close) * 100 >= 0.3  -- Week 1: Rise of at least 0.4%  
        AND ((close - prev_close) / prev_close) * 100 >= 0.3  -- Week 2: Rise of at least 0.4%  
)
SELECT 
    * ,
    (SELECT COUNT(*) FROM pattern_matches) AS total_occurrences,
    (SELECT COUNT(*) FROM pattern_matches WHERE third_week_drop = 'Yes') AS total_yes,
    (SELECT COUNT(*) FROM pattern_matches WHERE third_week_drop = 'No') AS total_no
FROM pattern_matches
ORDER BY symbol, week_2;
