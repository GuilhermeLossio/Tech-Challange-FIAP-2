-- Sample validation query
SELECT
  dt,
  ticker,
  volume_total_dia,
  mm7_preco
FROM default.b3_refined
WHERE ticker = 'GOLL4'
ORDER BY dt DESC
LIMIT 30;