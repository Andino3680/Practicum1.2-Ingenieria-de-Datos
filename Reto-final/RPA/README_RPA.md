# Integración RPA

En esta carpeta se incluyen los archivos SQL relacionados con la parte de RPA del proyecto.

Los archivos recibidos son:

- `tab_consolidado_supercias.sql`
- `tab_consolidado_export.sql`

Estos archivos contienen información consolidada en la tabla `TAB_CONSOLIDADO`, donde se almacenan registros extraídos o preparados por el proceso RPA.

Dentro del flujo del proyecto, RPA se considera una etapa previa al pipeline de datos. Primero RPA obtiene o consolida la información, luego los datos quedan disponibles para ser procesados por Python, y después se generan las tablas Silver y Gold que se cargan en SQLite y se usan en Power BI.

Flujo general:

RPA  
↓  
Archivos o datos consolidados  
↓  
Procesamiento con Python  
↓  
Tablas Silver y Gold  
↓  
SQLite  
↓  
Power BI