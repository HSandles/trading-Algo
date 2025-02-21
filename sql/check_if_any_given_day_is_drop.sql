-- THIS SCRIPT CHECKS IF ANY GIVEN DAY GIVES A DROP OF 0.14% FROM PREVIOUS DAY CLOSE

WITH price_changes AS (
    SELECT 
        symbol,
        DATE(time) AS day,  -- Extract date from the time column
        low,
        LAG(close, 1) OVER (PARTITION BY symbol ORDER BY time) AS prev_close
    FROM "1DAY_candles"
    WHERE tag = 'fx'
),
pattern_matches AS (
    SELECT 
        symbol,
        day,
        low AS day_low,
        prev_close,
        ((low - prev_close) / prev_close) * 100 AS percentage_change,
        CASE 
            WHEN ((low - prev_close) / prev_close) * 100 <= -0.14 THEN 'Yes'
            ELSE 'No'
        END AS drop_0_14
    FROM price_changes
    WHERE prev_close IS NOT NULL
)
SELECT 
    *,
    (SELECT COUNT(*) FROM pattern_matches) AS total_occurrences,
    (SELECT COUNT(*) FROM pattern_matches WHERE drop_0_14 = 'Yes') AS total_yes,
    (SELECT COUNT(*) FROM pattern_matches WHERE drop_0_14 = 'No') AS total_no
FROM pattern_matches
ORDER BY symbol, day;
