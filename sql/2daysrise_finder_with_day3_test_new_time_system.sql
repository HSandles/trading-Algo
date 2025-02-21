WITH price_changes AS (
    SELECT 
        symbol,
        trading_day,
        close,
        high,
        LAG(close, 1) OVER (PARTITION BY symbol ORDER BY trading_day) AS prev_close,
        LAG(close, 2) OVER (PARTITION BY symbol ORDER BY trading_day) AS prev_prev_close,
        LAG(close, 3) OVER (PARTITION BY symbol ORDER BY trading_day) AS prev_prev_prev_close,
        LEAD(high, 1) OVER (PARTITION BY symbol ORDER BY trading_day) AS next_high
    FROM fx_15_to_day_new_time_system
    
),
pattern_matches AS (
    SELECT 
        symbol,
        trading_day AS day_2,  
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
    symbol,
    day_2,
    day_2_close,
    day_3_high,
    third_day_rise,
    COUNT(*) OVER () AS total_occurrences,
    COUNT(*) FILTER (WHERE third_day_rise = 'Yes') OVER () AS total_yes,
    COUNT(*) FILTER (WHERE third_day_rise = 'No') OVER () AS total_no
FROM pattern_matches
ORDER BY symbol, day_2;
