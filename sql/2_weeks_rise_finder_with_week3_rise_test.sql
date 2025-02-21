-- THIS SCRIPT FINDS OCCURRENCES THAT MATCH MY 2 WEEKS RISE CRITERIA, AND THEN ALSO
-- CALCULATES WHETHER WEEK 3 WAS A RISE OR NOT BASED OFF THE HIGH OF WEEK 3 VS CLOSE WEEK 2
-- DAY = RISE IF +0.3%

WITH price_changes AS (
    SELECT 
        symbol,
        week_start,
        close,
        high,
        LAG(close, 1) OVER (PARTITION BY symbol ORDER BY week_start) AS prev_close,
        LAG(close, 2) OVER (PARTITION BY symbol ORDER BY week_start) AS prev_prev_close,
        LAG(close, 3) OVER (PARTITION BY symbol ORDER BY week_start) AS prev_prev_prev_close,
        LEAD(high, 1) OVER (PARTITION BY symbol ORDER BY week_start) AS next_high,  -- Week 3's high
        LEAD(close, 1) OVER (PARTITION BY symbol ORDER BY week_start) AS next_close -- Week 3's close
    FROM fx_15m_to_week
    WHERE tag = 'fx'
),
pattern_matches AS (
    SELECT 
        symbol,
        week_start AS week_2,  
        close AS week_2_close,
        next_high AS week_3_high,
        next_close AS week_3_close,
        CASE 
            WHEN ((next_high - close) / close) * 100 >= 0.3 THEN 'Yes'
            ELSE 'No'
        END AS third_week_rise
    FROM price_changes
    WHERE 
        ((prev_prev_close - prev_prev_prev_close) / prev_prev_prev_close) * 100 BETWEEN -6 AND 0.15  -- Week 0: No net change or drop  
        AND ((prev_close - prev_prev_close) / prev_prev_close) * 100 >= 0.3  -- Week 1: Rise of at least 0.3%  
        AND ((close - prev_close) / prev_close) * 100 >= 0.3  -- Week 2: Rise of at least 0.3%  
)
SELECT 
    * ,
    (SELECT COUNT(*) FROM pattern_matches) AS total_occurrences,
    (SELECT COUNT(*) FROM pattern_matches WHERE third_week_rise = 'Yes') AS total_yes,
    (SELECT COUNT(*) FROM pattern_matches WHERE third_week_rise = 'No') AS total_no
FROM pattern_matches
ORDER BY symbol, week_2;
