
-- Consulta 1: Verificar que la tabla limpia existe y tiene datos

SELECT *
FROM instituciones_limpias
LIMIT 10;

-- =====================================================
-- Consulta 2: Matrícula total por provincia 2023-2024
-- Pregunta:
-- ¿Cómo se distribuye la matrícula total por provincia
-- en el año lectivo más reciente?
-- =====================================================

SELECT 
    provincia,
    SUM(total_estudiantes) AS matricula_total
FROM instituciones_limpias
WHERE anio_lectivo = '2023-2024 Inicio'
GROUP BY provincia
ORDER BY matricula_total DESC;

-- =====================================================
-- Consulta 3: Instituciones fiscales vs particulares
-- por área urbana/rural en Loja
-- =====================================================

SELECT 
    area,
    sostenimiento,
    COUNT(cod_amie) AS total_instituciones
FROM instituciones_limpias
WHERE provincia = 'LOJA'
  AND anio_lectivo = '2023-2024 Inicio'
  AND sostenimiento IN ('Fiscal', 'Particular')
GROUP BY area, sostenimiento
ORDER BY area, sostenimiento;

-- =====================================================
-- Consulta 4: Evolución de instituciones activas
-- en Ecuador entre 2015 y 2024
-- =====================================================

SELECT 
    anio_lectivo,
    COUNT(DISTINCT cod_amie) AS instituciones_activas
FROM instituciones_limpias
WHERE anio_lectivo IN (
    '2015-2016 Inicio',
    '2016-2017 Inicio',
    '2017-2018 Inicio',
    '2018-2019 Inicio',
    '2019-2020 Inicio',
    '2020-2021 Inicio',
    '2021-2022 Inicio',
    '2022-2023 Inicio',
    '2023-2024 Inicio'
)
GROUP BY anio_lectivo
ORDER BY anio_lectivo;


-- =====================================================
-- Consulta 5: KPI de matrícula nacional 2023-2024
-- =====================================================

SELECT 
    SUM(total_estudiantes) AS matricula_nacional_2023_2024
FROM instituciones_limpias
WHERE anio_lectivo = '2023-2024 Inicio';


-- =====================================================
-- Consulta 6: Ver años lectivos disponibles
-- Sirve para revisar que no se incluyan registros
-- de Costa Revisión o Sierra Revisión.
-- =====================================================

SELECT 
    anio_lectivo,
    COUNT(*) AS total_registros
FROM instituciones_limpias
GROUP BY anio_lectivo
ORDER BY anio_lectivo;