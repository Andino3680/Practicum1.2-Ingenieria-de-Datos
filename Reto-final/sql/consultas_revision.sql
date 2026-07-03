-- 1. Ver tablas cargadas
SELECT name 
FROM sqlite_master 
WHERE type = 'table'
ORDER BY name;

-- 2. Conteo de registros por tabla
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

-- 3. Provincias con más bachilleres
SELECT 
    provincia,
    total_bachilleres_3ro,
    instituciones_bachillerato,
    total_estudiantes
FROM gold_bachilleres_provincia
ORDER BY total_bachilleres_3ro DESC
LIMIT 10;

-- 4. Evolución del PIB real
SELECT 
    anio,
    pib_real_musd,
    variacion_pib_pct,
    clasificacion
FROM gold_pib_tendencia
ORDER BY anio;

-- 5. Últimos datos de petróleo y riesgo país
SELECT 
    fecha,
    precio_petroleo_wti,
    riesgo_pais_pb,
    promedio_wti_30dias
FROM gold_petroleo_30dias
ORDER BY fecha DESC
LIMIT 15;

-- 6. VAB por provincia en 2023
SELECT 
    provincia,
    SUM(vab_total_usd) AS total_vab_2023
FROM gold_vab_provincia_sector
WHERE anio = 2023
GROUP BY provincia
ORDER BY total_vab_2023 DESC
LIMIT 10;

-- 7. Sectores con más VAB en 2023
SELECT 
    sector,
    SUM(vab_total_usd) AS total_vab_2023
FROM gold_vab_provincia_sector
WHERE anio = 2023
GROUP BY sector
ORDER BY total_vab_2023 DESC
LIMIT 10;