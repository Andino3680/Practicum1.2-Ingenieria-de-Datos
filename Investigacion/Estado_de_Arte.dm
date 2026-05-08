Práctica con énfasis en Ingeniería de Datos

Nombre:Raúl Andino

I.ABSTRACT

Este artículo examina la situación actual de la ingeniería de datos contemporánea y su relevancia en los procedimientos de transformación digital en contextos empresariales e institucionales. Se analizan ideas vinculadas a la calidad de datos, procesamiento distribuido, tuberías de datos, API y ETL/ELT, así como también instrumentos que se usan mucho como dbt, Apache Spark y Prefect. A través de una revisión bibliográfica que incluye documentos técnicos y artículos especializados, se determinan las metodologías y tecnologías más utilizadas para procesar y automatizar grandes cantidades de datos. Se estudian tendencias como la observación de datos, la integración de inteligencia artificial en tuberías modernas y las arquitecturas Lakehouse. En conclusión, la Ingeniería de Datos es un elemento esencial para mejorar procesos y robustecer las decisiones basadas en datos.

El presente artículo analiza el estado del arte de la Ingeniería de Datos

II.INTRODUCCION

Hoy en dia las empresas y las instituciones manegan grandes cantidadaes de datos que provienen de plataformas tecnologicas, sistemas diguitales y servicios administrattivos la demanda de construir arquitecturas modernas que hagan posible el procesamiento, la transformacion y el almacenamiento de informacion de forma segura y eficaz ha surgido del crecimiento de continuo de los datos.

En este contexto, la Ingeniería de Datos ha tomado un rol fundamental en los procesos de transformación digital; esto se debe a que permite la automatización de pipelines, la integración de varias fuentes de datos y la garantía de que la información esté disponible y sea confiable

La Ingeniería de Datos es un componente esencial en la Dirección General de Tecnologías de la Información y Transformación Digital (DGTITD) de la Universidad Técnica Particular de Loja (UTPL), ya que permite optimizar procesos dentro de la institución, automatizar informes y reforzar el soporte tecnológico en tareas académicas y administrativas.

III.MARCO CONCEPTUAL

3.1 Ingeniería de Datos
La ingeniería de datos es la práctica de diseñar y construir sistemas para la agregación, el almacenamiento y el análisis de datos a escala. Los ingenieros de datos permiten a las organizaciones obtener conocimientos en tiempo real a partir de grandes conjuntos de datos.
Las organizaciones tienen acceso a más datos (y a más tipos de datos) que nunca. Cada bit de datos puede potencialmente informar una decisión comercial crucial. Los ingenieros de datos se encargan de la gestión de los datos para su uso posterior, incluidos el análisis, la previsión o el machine learning.

3.2 ETL y ELT
¿Qué es ETL?
Extracción, transformación y carga (ETL) es el proceso consistente en combinar datos de diferentes orígenes un gran repositorio central llamado almacenamiento de datos. ETL utiliza un conjunto de reglas comerciales para limpiar y organizar datos en bruto y prepararlos para el almacenamiento, el análisis de datos y el machine learning (ML). Puede abordar necesidades de inteligencia empresarial específicas mediante análisis de datos (como la predicción del resultado de decisiones empresariales, la generación de informes y paneles, la reducción de la ineficacia operativa y más).

¿Qué es ELT?
La extracción, carga y transformación (ELT) es una extensión de la extracción, transformación y carga (ETL) que invierte el orden de las operaciones. Puede cargar datos directamente en el sistema de destino antes de procesarlos. El área de preparación intermedia no es necesaria porque el almacenamiento de datos de destino tiene capacidades de asignación de datos dentro de él. ELT se ha vuelto más popular con la adopción de la infraestructura en la nube, que brinda a las bases de datos de destino la potencia de procesamiento que necesitan para las transformaciones.

3.3 APIs y Fuentes de Datos
Una API, o interfaz de programación de aplicaciones, es un conjunto de reglas o protocolos que permiten que las aplicaciones de software se comuniquen entre sí para intercambiar datos, características y funcionalidades.
Las API simplifican y aceleran la aplicación y desarrollo de software permitiendo a los desarrolladores integrar datos, servicios y capacidades de otras aplicaciones, en lugar de desarrollarlos desde cero. Las API brindan a los propietarios de aplicaciones una forma simple y segura de hacer que los datos y funciones de las aplicaciones estén disponibles para los consumidores de API internos y externos.

Tipos de API:
API web, API abiertas, API de socios, API internas, API compuestas, API de datos (o bases de datos), API del sistema operativo (o local),API remotas.

3.4 Pipelines de Datos
Una canalización de datos es un sistema para recuperar datos de varias fuentes y canalizarlos a una nueva ubicación, como una base de datos, un repositorio o una aplicación, y realizar cualquier transformación de datos necesaria (convertir datos de un formato o estructura a otro) a lo largo del camino. Los usuarios finales de este proceso varían desde analistas hasta científicos de datos y líderes empresariales.

3.5 Apache Spark
Apache Spark™ es un motor multilingüe para ejecutar ingeniería de datos, ciencia de datos y aprendizaje automático en máquinas o clústeres de un solo nodo.

Características clave
Datos por lotes/transmisión
Unifica el procesamiento de tus datos en lotes y streaming en tiempo real, utilizando tu lenguaje preferido: Python, SQL, Scala, Java o R.
Análisis SQL
Ejecute consultas ANSI SQL rápidas y distribuidas para paneles e informes ad hoc. Funciona más rápido que la mayoría de los almacenes de datos.
Ciencia de datos a escala
Realice un análisis exploratorio de datos (EDA) en datos a escala de petabytes sin tener que recurrir a la reducción de muestreo
Aprendizaje automático
Entrene algoritmos de aprendizaje automático en una computadora portátil y utilice el mismo código para escalar a grupos de miles de máquinas tolerantes a fallas.

3.6 Prefect
Orquestar flujos de trabajo. Cree aplicaciones de IA. Fundaciones de código abierto, plataformas listas para producción.

3.7 dbt
Copiloto es un potente asistente impulsado por IA totalmente integrado en su dbt experiencia—diseñada para acelerar sus flujos de trabajo de análisis.

Copiloto Incorpora asistencia impulsada por IA en cada etapa del ciclo de vida del desarrollo analítico (ADLC) y aprovecha metadatos enriquecidos —capturando relaciones, linaje y contexto— para que pueda ofrecer productos de datos refinados y confiables a gran velocidad.

3.8 Calidad y Observabilidad de Datos
La observabilidad de datos de IBM transforma las operaciones de datos al abordar de forma proactiva los procesos rotos, los retrasos y los problemas de calidad, lo que aumenta significativamente la productividad del equipo y ofrece un retorno de la inversión claro para su negocio. Ofrece monitoreo proactivo, datos históricos completos y linaje completo, reduciendo drásticamente el tiempo medio de detección (MTTD) a minutos y el tiempo medio de reparación (MTTR) a horas o menos.

IV.TRABAJOS RELACIONADOS
Amazon EMR + Spark
Apache Sparkes un marco de procesamiento distribuido y un modelo de programación que ayuda Realiza aprendizaje automático, procesamiento de flujo o análisis de gráficos con clústeres de Amazon EMR. Similar Para Apache Hadoop, Spark es un sistema de procesamiento distribuido de código abierto comúnmente utilizado para cargas de trabajo de big data.

Spark Streaming
flujos Spark. Medimos la latencia de la transmisión y monitoreamos la escalabilidad agregando y eliminando nodos en medio de un trabajo de transmisión. También verificamos la tolerancia a fallas deteniendo los nodos en medio de un trabajo y asegurándonos de que el trabajo se reprograme y complete en otros nodos.Diseñamos una aplicación completa que automatiza la recopilación de datos

V. Herramientas y tecnologías

| HERRAMIENTA    | FUNCIÓN PRINCIPAL                           | CARACTERÍSTICA DESTACADA                                                                          |
| -------------- | ------------------------------------------- | ------------------------------------------------------------------------------------------------- |
| Apache Spark   | Procesamiento distribuido de datos          | Permite trabajar con grandes volúmenes de información de manera rápida y escalable.               |
| Prefect        | Orquestación y automatización de pipelines  | Facilita la programación, monitoreo y control de flujos de datos utilizando Python.               |
| dbt            | Transformación analítica de datos           | Utiliza SQL modular y versionado para organizar transformaciones de datos.                        |
| Apache Airflow | Gestión de workflows                        | Coordina tareas complejas mediante estructuras DAG para automatización de procesos.               |
| Python         | Automatización y procesamiento de datos     | Posee librerías especializadas como pandas y PySpark para análisis y manipulación de información. |
| SQL            | Consulta y administración de bases de datos | Lenguaje estándar utilizado para acceder y manipular datos estructurados.                         |
| Power BI       | Visualización e interpretación de datos     | Permite crear dashboards interactivos y reportes institucionales.                                 |
| Tableau        | Análisis visual y storytelling              | Facilita la representación gráfica de datos para apoyar la toma de decisiones.                    |

VI . Tendencias y futuro
¿Por qué 2026 redefinirá la ingeniería de datos como una disciplina nativa de la IA?
Durante el año pasado, la IA pasó de la experimentación a flujos de trabajo reales, revelando una verdad simple: un modelo de IA es tan bueno como los datos que lo impulsan. La ingeniería de datos ya no es una función detrás del escenario. Se está convirtiendo en la columna vertebral de la inteligencia empresarial y en un determinante estratégico de hasta qué punto las organizaciones pueden escalar la IA

VII . Conclusiones
La Ingeniería de Datos se ha vuelto un campo fundamental en el proceso de transformación digital, por su habilidad para manejar, procesar y convertir enormes cantidades de información que llegan de diversas fuentes tecnológicas. Hoy en día, las organizaciones necesitan estructuras modernas y herramientas especializadas que faciliten la automatización de procesos y aseguren la disponibilidad y calidad de los datos.

El estudio demuestra que tecnologías como Apache Spark, Prefect, dbt y plataformas de visualización de datos son esenciales para crear pipelines modernos, ya que permiten el procesamiento distribuido, la automatización y la integración eficaz de información. Además, la implementación de arquitecturas escalables y APIs facilita la optimización de procesos vinculados con el análisis, el almacenamiento y la toma de decisiones.

Dentro de la Dirección General de Tecnologías de la Información y Transformación Digital (DGTITD) de la UTPL, la Ingeniería de Datos representa una herramienta estratégica para fortalecer la automatización institucional, mejorar la gestión de información académica y administrativa, y apoyar procesos de análisis basados en datos confiables y actualizados.

VIII. REFERENCIAS
IBM – What Is Data Engineering?. (s.f.). What is data engineering? IBM. https://www.ibm.com/es-es/think/topics/data-engineering
AWS – What is ETL?. (s.f.). What is ETL? Amazon Web Services. https://aws.amazon.com/what-is/etl/
IBM – What is an API?. (s.f.). What is an API? IBM. https://www.ibm.com/think/topics/api
DataCamp – Introduction to Data Pipelines. (s.f.). Introduction to data pipelines for data professionals. DataCamp. https://www.datacamp.com/tutorial/introduction-to-data-pipelines-for-data-professionals
Apache Spark Official Site. (s.f.). Apache Spark. Apache Software Foundation. https://spark.apache.org/
Prefect Official Website. (s.f.). Prefect. Prefect Technologies. https://www.prefect.io/
dbt Documentation. (s.f.). dbt documentation. dbt Labs. https://docs.getdbt.com/docs/platform/dbt-copilot?version=1.12
IBM – Data Observability. (s.f.). Smart data engineering: How observability drives productivity and efficiency. IBM. https://www.ibm.com/new/product-blog/smart-data-engineering-how-observability-drives-productivity-and-efficiency
AWS EMR Spark Documentation. (s.f.). Apache Spark in Amazon EMR. Amazon Web Services. https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-spark.html
Distributed Streaming Analytics using Apache Spark. (2019). Distributed streaming analytics using Apache Spark. arXiv. https://arxiv.org/abs/1907.13264
AI-Native Data Engineering Discipline. (2026). Why 2026 will redefine data engineering as an AI-native discipline. CDO Magazine. https://www.cdomagazine.tech/opinion-analysis/why-2026-will-redefine-data-engineering-as-an-ai-native-discipline
