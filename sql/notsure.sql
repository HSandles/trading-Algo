WITH price_changes AS (
    SELECT 
        symbol,
        day,
        close,
        LAG(close, 1) OVER (PARTITION BY symbol ORDER BY day) AS prev_close
    FROM fx_15m_to_day
    WHERE tag = 'fx'
),
rise_counts AS (
    SELECT 
        symbol,
        COUNT(*) AS days_with_rise
    FROM price_changes
    WHERE ((close - prev_close) / prev_close) * 100 >= 0.14
    GROUP BY symbol
)
SELECT 
    p.symbol,
    COUNT(*) AS days_tested,
    COALESCE(r.days_with_rise, 0) AS days_with_rise,
    COUNT(*) - COALESCE(r.days_with_rise, 0) AS days_without_rise
FROM price_changes p
LEFT JOIN rise_counts r ON p.symbol = r.symbol
GROUP BY p.symbol, r.days_with_rise
ORDER BY p.symbol;
