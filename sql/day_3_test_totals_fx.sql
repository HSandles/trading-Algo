-- THIS SCRIPT TOTALS THE YES/NO FOR 3RD DAY RISE OR DROP BY FX PAIR
-- YOU NED TO CHANGE RISE/DROP

SELECT 
    symbol,
    COUNT(*) AS total_occurrences,
    COUNT(*) FILTER (WHERE third_day_rise = 'Yes') AS total_yes,
    COUNT(*) FILTER (WHERE third_day_rise = 'No') AS total_no
FROM fx_3rd_day_rise
GROUP BY symbol
ORDER BY symbol;