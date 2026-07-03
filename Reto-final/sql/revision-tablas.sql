SELECT 'silver_pib_nominal' AS tabla, COUNT(*) AS registros FROM silver_pib_nominal
UNION ALL
SELECT 'silver_pib_real', COUNT(*) FROM silver_pib_real
UNION ALL
SELECT 'silver_petroleo', COUNT(*) FROM silver_petroleo
UNION ALL
SELECT 'silver_riesgo_pais', COUNT(*) FROM silver_riesgo_pais
UNION ALL
SELECT 'silver_iee', COUNT(*) FROM silver_iee
UNION ALL
SELECT 'silver_vab', COUNT(*) FROM silver_vab
UNION ALL
SELECT 'silver_mineduc_bachillerato', COUNT(*) FROM silver_mineduc_bachillerato
UNION ALL
SELECT 'gold_pib_tendencia', COUNT(*) FROM gold_pib_tendencia
UNION ALL
SELECT 'gold_petroleo_30dias', COUNT(*) FROM gold_petroleo_30dias
UNION ALL
SELECT 'gold_vab_provincia_sector', COUNT(*) FROM gold_vab_provincia_sector
UNION ALL
SELECT 'gold_bachilleres_provincia', COUNT(*) FROM gold_bachilleres_provincia;