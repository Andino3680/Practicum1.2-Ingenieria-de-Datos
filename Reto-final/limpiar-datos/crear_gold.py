import re
import sqlite3
import unicodedata
from pathlib import Path

import pandas as pd


# =========================================================
# RUTAS DEL PROYECTO
# =========================================================

PROYECTO = Path(__file__).resolve().parent.parent

RUTA_DB = (
    PROYECTO
    / "base-datos"
    / "macroentorno.db"
)

# Carpeta nueva exclusiva para los Excel
CARPETA_EXCEL = (
    PROYECTO
    / "salida-excel"
)

CARPETA_EXCEL.mkdir(
    parents=True,
    exist_ok=True
)

ARCHIVO_EXCEL = (
    CARPETA_EXCEL
    / "gold_macroentorno.xlsx"
)


# =========================================================
# CONEXIÓN A SQLITE
# =========================================================

conexion = sqlite3.connect(
    RUTA_DB
)


# =========================================================
# CREAR ARCHIVO EXCEL
# =========================================================

writer_excel = pd.ExcelWriter(
    ARCHIVO_EXCEL,
    engine="openpyxl"
)


# =========================================================
# FUNCIONES GENERALES
# =========================================================

def leer_tabla(nombre):
    """
    Lee una tabla completa desde SQLite.
    """

    return pd.read_sql_query(
        f"SELECT * FROM {nombre}",
        conexion
    )


def guardar_gold(df, nombre):
    """
    Guarda cada tabla Gold:
    1. En macroentorno.db.
    2. En una hoja del archivo Excel.
    """

    # Guardar en SQLite
    df.to_sql(
        nombre,
        conexion,
        if_exists="replace",
        index=False
    )

    # Guardar en Excel
    df.to_excel(
        writer_excel,
        sheet_name=nombre[:31],
        index=False
    )

    print(
        f"{nombre}: {len(df)} registros"
    )


def convertir_numero(serie):
    """
    Convierte una columna a valores numéricos.
    """

    if pd.api.types.is_numeric_dtype(serie):
        return serie

    texto = (
        serie
        .astype(str)
        .str.strip()
        .str.replace(",", ".", regex=False)
    )

    return pd.to_numeric(
        texto,
        errors="coerce"
    )


def normalizar_texto(valor):
    """
    Normaliza textos para facilitar cruces,
    especialmente nombres de provincias.
    """

    if pd.isna(valor):
        return None

    texto = str(valor).strip().upper()

    texto = unicodedata.normalize(
        "NFKD",
        texto
    )

    texto = "".join(
        caracter
        for caracter in texto
        if not unicodedata.combining(caracter)
    )

    texto = re.sub(
        r"\s+",
        " ",
        texto
    )

    return texto.strip()


def buscar_columna(df, candidatos):
    """
    Busca la primera columna disponible
    entre diferentes nombres posibles.
    """

    for candidato in candidatos:

        if candidato in df.columns:
            return candidato

    return None


print()
print("=" * 60)
print("CREANDO CAPA GOLD")
print("=" * 60)


# =========================================================
# 1. GOLD PIB TENDENCIA
# =========================================================

print()
print("Procesando PIB...")

pib_real = leer_tabla(
    "silver_pib_real"
)

pib_nominal = leer_tabla(
    "silver_pib_nominal"
)


columnas_real = [
    "anio_fiscal",
    "pib_real_millones",
    "poblacion_total",
    "pib_pc_real_usd",
    "tasa_variacion_anual"
]


gold_pib = pib_real[
    columnas_real
].copy()


# Convertir año

gold_pib[
    "anio_fiscal"
] = convertir_numero(
    gold_pib[
        "anio_fiscal"
    ]
)


# Convertir columnas numéricas

for columna in [
    "pib_real_millones",
    "poblacion_total",
    "pib_pc_real_usd",
    "tasa_variacion_anual"
]:

    gold_pib[
        columna
    ] = convertir_numero(
        gold_pib[
            columna
        ]
    )


# Agregar PIB nominal per cápita

if (
    "pib_pc_nominal_usd"
    in pib_nominal.columns
):

    nominal = pib_nominal[
        [
            "anio_fiscal",
            "pib_pc_nominal_usd"
        ]
    ].copy()


    nominal[
        "anio_fiscal"
    ] = convertir_numero(
        nominal[
            "anio_fiscal"
        ]
    )


    nominal[
        "pib_pc_nominal_usd"
    ] = convertir_numero(
        nominal[
            "pib_pc_nominal_usd"
        ]
    )


    gold_pib = gold_pib.merge(
        nominal,
        on="anio_fiscal",
        how="left"
    )


# Clasificación del ciclo económico

def clasificar_pib(valor):

    if pd.isna(valor):
        return "Sin información"

    if valor > 2:
        return "Crecimiento fuerte"

    if valor > 0:
        return "Crecimiento moderado"

    if valor == 0:
        return "Estancamiento"

    return "Contracción"


gold_pib[
    "clasificacion"
] = gold_pib[
    "tasa_variacion_anual"
].apply(
    clasificar_pib
)


gold_pib = gold_pib.sort_values(
    "anio_fiscal"
)


guardar_gold(
    gold_pib,
    "gold_pib_tendencia"
)


# =========================================================
# 2. GOLD PETRÓLEO 30 DÍAS
# =========================================================

print()
print(
    "Procesando petróleo y riesgo país..."
)

petroleo = leer_tabla(
    "silver_petroleo"
)

riesgo = leer_tabla(
    "silver_riesgo_pais"
)


# -------------------------------
# PETRÓLEO
# -------------------------------

petroleo = petroleo[
    [
        "fecha_fiscal",
        "valor"
    ]
].copy()


petroleo.columns = [
    "fecha",
    "precio_petroleo_wti"
]


petroleo[
    "fecha"
] = pd.to_datetime(
    petroleo[
        "fecha"
    ],
    errors="coerce"
)


petroleo[
    "precio_petroleo_wti"
] = convertir_numero(
    petroleo[
        "precio_petroleo_wti"
    ]
)


petroleo = petroleo.dropna(
    subset=[
        "fecha"
    ]
)


petroleo = petroleo.sort_values(
    "fecha"
)


# Promedio móvil 30 días

petroleo[
    "promedio_movil_30d"
] = (
    petroleo[
        "precio_petroleo_wti"
    ]
    .rolling(
        window=30,
        min_periods=1
    )
    .mean()
)


# -------------------------------
# RIESGO PAÍS
# -------------------------------

riesgo = riesgo[
    [
        "fecha_fiscal",
        "valor"
    ]
].copy()


riesgo.columns = [
    "fecha",
    "riesgo_pais_pb"
]


riesgo[
    "fecha"
] = pd.to_datetime(
    riesgo[
        "fecha"
    ],
    errors="coerce"
)


riesgo[
    "riesgo_pais_pb"
] = convertir_numero(
    riesgo[
        "riesgo_pais_pb"
    ]
)


riesgo = riesgo.dropna(
    subset=[
        "fecha"
    ]
)


# Combinar petróleo y riesgo país

gold_petroleo = petroleo.merge(
    riesgo,
    on="fecha",
    how="outer"
)


gold_petroleo = gold_petroleo.sort_values(
    "fecha"
)


guardar_gold(
    gold_petroleo,
    "gold_petroleo_30dias"
)


# =========================================================
# 3. GOLD IEE TENDENCIA
# =========================================================

print()
print("Procesando IEE...")

iee = leer_tabla(
    "silver_iee"
)


columnas_iee = {

    "periodo_fiscal":
        "periodo",

    "fecha_publicacion":
        "fecha",

    "metricas_iee_global":
        "iee_global",

    "metricas_comercio":
        "comercio",

    "metricas_construccion":
        "construccion",

    "metricas_manufactura":
        "manufactura",

    "metricas_servicios":
        "servicios"
}


columnas_existentes = {

    columna: nuevo_nombre

    for columna, nuevo_nombre
    in columnas_iee.items()

    if columna in iee.columns
}


gold_iee = iee[
    list(
        columnas_existentes.keys()
    )
].copy()


gold_iee = gold_iee.rename(
    columns=
        columnas_existentes
)


if (
    "fecha"
    in gold_iee.columns
):

    gold_iee[
        "fecha"
    ] = pd.to_datetime(
        gold_iee[
            "fecha"
        ],
        errors="coerce"
    )


for columna in [

    "iee_global",
    "comercio",
    "construccion",
    "manufactura",
    "servicios"

]:

    if (
        columna
        in gold_iee.columns
    ):

        gold_iee[
            columna
        ] = convertir_numero(
            gold_iee[
                columna
            ]
        )


guardar_gold(
    gold_iee,
    "gold_iee_tendencia"
)


# =========================================================
# 4. GOLD VAB POR PROVINCIA
# =========================================================

print()
print("Procesando VAB...")

vab = leer_tabla(
    "silver_vab"
)


vab[
    "provincia_normalizada"
] = vab[
    "provincia"
].apply(
    normalizar_texto
)


columna_total_vab = (
    "sectores_economia_total"
)


if (
    columna_total_vab
    in vab.columns
):

    vab[
        columna_total_vab
    ] = convertir_numero(
        vab[
            columna_total_vab
        ]
    )


    gold_vab_provincia = (
        vab.groupby(
            [
                "provincia_normalizada",
                "provincia"
            ],
            dropna=False
        )[
            columna_total_vab
        ]
        .sum()
        .reset_index()
    )


    gold_vab_provincia = (
        gold_vab_provincia.rename(
            columns={
                columna_total_vab:
                    "vab_total"
            }
        )
    )


    guardar_gold(
        gold_vab_provincia,
        "gold_vab_provincia"
    )


# =========================================================
# 5. GOLD VAB POR PROVINCIA Y SECTOR
# =========================================================

columnas_sector = [

    columna

    for columna in vab.columns

    if columna.startswith(
        "sectores_"
    )

    and columna
    != "sectores_economia_total"
]


for columna in columnas_sector:

    vab[
        columna
    ] = convertir_numero(
        vab[
            columna
        ]
    )


gold_vab_sector = vab.melt(

    id_vars=[
        "provincia_normalizada",
        "provincia",
        "canton"
    ],

    value_vars=
        columnas_sector,

    var_name=
        "sector",

    value_name=
        "vab"
)


gold_vab_sector[
    "sector"
] = (
    gold_vab_sector[
        "sector"
    ]
    .str.replace(
        "sectores_",
        "",
        regex=False
    )
    .str.replace(
        "_",
        " ",
        regex=False
    )
)


gold_vab_sector = (
    gold_vab_sector.groupby(
        [
            "provincia_normalizada",
            "provincia",
            "sector"
        ],
        dropna=False
    )[
        "vab"
    ]
    .sum()
    .reset_index()
)


guardar_gold(
    gold_vab_sector,
    "gold_vab_provincia_sector"
)


# =========================================================
# 6. GOLD EMPRESAS POR PROVINCIA
# =========================================================

print()
print("Procesando empresas...")

empresas = leer_tabla(
    "silver_supercias_directorio"
)


empresas[
    "provincia_normalizada"
] = empresas[
    "ubicacion_provincia"
].apply(
    normalizar_texto
)


columna_empresa = buscar_columna(

    empresas,

    [
        "empresa_metadata_ruc",
        "empresa_metadata_expediente"
    ]
)


columna_estado = (
    "empresa_metadata_situacion_legal"
)


empresas[
    "empresa_id"
] = empresas[
    columna_empresa
]


# Identificar empresas activas

if (
    columna_estado
    in empresas.columns
):

    estados = (
        empresas[
            columna_estado
        ]
        .fillna("")
        .astype(str)
        .str.upper()
    )


    empresas[
        "es_activa"
    ] = estados.str.contains(

        r"\bACTIV[AO]\b",

        regex=True
    )


else:

    empresas[
        "es_activa"
    ] = True


# Total empresas

total_empresas = (
    empresas.groupby(
        "provincia_normalizada"
    )[
        "empresa_id"
    ]
    .nunique()
    .reset_index(
        name=
            "total_empresas"
    )
)


# Empresas activas

empresas_activas = (
    empresas[
        empresas[
            "es_activa"
        ]
    ]
    .groupby(
        "provincia_normalizada"
    )[
        "empresa_id"
    ]
    .nunique()
    .reset_index(
        name=
            "empresas_activas"
    )
)


gold_empresas = (
    total_empresas.merge(

        empresas_activas,

        on=
            "provincia_normalizada",

        how=
            "left"
    )
)


gold_empresas[
    "empresas_activas"
] = gold_empresas[
    "empresas_activas"
].fillna(
    0
)


guardar_gold(
    gold_empresas,
    "gold_empresas_provincia"
)


# =========================================================
# 7. GOLD BACHILLERES POR PROVINCIA
# =========================================================

print()
print("Procesando MINEDUC...")

mineduc = leer_tabla(
    "silver_mineduc"
)


mineduc[
    "provincia_normalizada"
] = mineduc[
    "institucion_provincia"
].apply(
    normalizar_texto
)


# Buscar nivel educativo

columna_nivel = buscar_columna(

    mineduc,

    [
        "institucion_nivel_educacion",
        "nivel_educacion"
    ]
)


# Buscar columna de estudiantes

candidatos_estudiantes = [

    "total_estudiantes",
    "institucion_total_estudiantes",
    "estudiantes_total",
    "matricula_total",
    "total_alumnos",
    "numero_estudiantes"
]


columna_estudiantes = buscar_columna(

    mineduc,

    candidatos_estudiantes
)


# Búsqueda automática

if (
    columna_estudiantes
    is None
):

    for columna in mineduc.columns:

        nombre = columna.lower()

        if (

            "estudiant"
            in nombre

            or "matricula"
            in nombre

            or "alumnos"
            in nombre

        ):

            if (

                "hombre"
                not in nombre

                and "mujer"
                not in nombre

            ):

                columna_estudiantes = (
                    columna
                )

                break


# Filtrar Bachillerato

if (
    columna_nivel
    is not None
):

    filtro_bachillerato = (

        mineduc[
            columna_nivel
        ]
        .fillna("")
        .astype(str)
        .str.upper()
        .str.contains(
            "BACHILL"
        )
    )


    bachillerato = mineduc[
        filtro_bachillerato
    ].copy()


else:

    bachillerato = mineduc.copy()


# Sumar estudiantes

if (
    columna_estudiantes
    is not None
):

    print(
        "Columna utilizada para estudiantes:",
        columna_estudiantes
    )


    bachillerato[
        columna_estudiantes
    ] = convertir_numero(

        bachillerato[
            columna_estudiantes
        ]
    )


    gold_bachilleres = (
        bachillerato.groupby(
            "provincia_normalizada"
        )[
            columna_estudiantes
        ]
        .sum()
        .reset_index()
    )


    gold_bachilleres = (
        gold_bachilleres.rename(
            columns={
                columna_estudiantes:
                    "total_bachilleres"
            }
        )
    )


else:

    print(
        "No se encontró una columna "
        "con el total de estudiantes."
    )


    gold_bachilleres = (
        bachillerato.groupby(
            "provincia_normalizada"
        )[
            "institucion_amie"
        ]
        .nunique()
        .reset_index(
            name=
                "instituciones_bachillerato"
        )
    )


guardar_gold(
    gold_bachilleres,
    "gold_bachilleres_provincia"
)


# =========================================================
# 8. GOLD BACHILLERES VS EMPRESAS
# =========================================================

print()

print(
    "Creando Bachilleres vs Empresas..."
)


gold_bachilleres_empresas = (

    gold_bachilleres.merge(

        gold_empresas,

        on=
            "provincia_normalizada",

        how=
            "outer"
    )
)


if (

    "total_bachilleres"

    in gold_bachilleres_empresas.columns
):

    gold_bachilleres_empresas[
        "ratio_bachilleres_por_empresa"
    ] = (

        gold_bachilleres_empresas[
            "total_bachilleres"
        ]

        /

        gold_bachilleres_empresas[
            "empresas_activas"
        ].replace(
            0,
            pd.NA
        )
    )


guardar_gold(
    gold_bachilleres_empresas,
    "gold_bachilleres_vs_empresas"
)


# =========================================================
# 9. GOLD EMPLEO TENDENCIA
# =========================================================

print()
print("Procesando ENEMDU...")

enemdu = leer_tabla(
    "silver_enemdu"
)


columna_nacional = None
columna_urbana = None
columna_rural = None


for columna in enemdu.columns:

    nombre = columna.lower()


    if (

        columna_nacional
        is None

        and "nacional"
        in nombre

    ):

        columna_nacional = columna


    if (

        columna_urbana
        is None

        and "urbana"
        in nombre

    ):

        columna_urbana = columna


    if (

        columna_rural
        is None

        and "rural"
        in nombre

    ):

        columna_rural = columna


columnas_empleo = [

    columna

    for columna in [

        "encuesta",
        "periodo_original",
        "anio_fiscal",
        "mes_fiscal",
        "nombre_indicador",
        columna_nacional,
        columna_urbana,
        columna_rural

    ]

    if columna is not None

    and columna
    in enemdu.columns
]


gold_empleo = enemdu[
    columnas_empleo
].copy()


renombres = {}


if columna_nacional:

    renombres[
        columna_nacional
    ] = "total_nacional"


if columna_urbana:

    renombres[
        columna_urbana
    ] = "total_urbana"


if columna_rural:

    renombres[
        columna_rural
    ] = "total_rural"


gold_empleo = gold_empleo.rename(
    columns=
        renombres
)


guardar_gold(
    gold_empleo,
    "gold_empleo_tendencia"
)


# =========================================================
# 10. GOLD EMPLEO POR RAMA
# =========================================================

print()

print(
    "Procesando empleo por rama..."
)


censo = leer_tabla(
    "silver_censo_rama_actividad"
)


columnas_ramas = [

    columna

    for columna in censo.columns

    if columna.startswith(
        "ramas_actividad_"
    )
]


for columna in columnas_ramas:

    censo[
        columna
    ] = convertir_numero(
        censo[
            columna
        ]
    )


gold_empleo_ciiu = censo.melt(

    id_vars=[
        "anio_censo",
        "provincia",
        "canton"
    ],

    value_vars=
        columnas_ramas,

    var_name=
        "sector",

    value_name=
        "personas_ocupadas"
)


gold_empleo_ciiu[
    "sector"
] = (

    gold_empleo_ciiu[
        "sector"
    ]

    .str.replace(
        "ramas_actividad_",
        "",
        regex=False
    )

    .str.replace(
        "_",
        " ",
        regex=False
    )
)


gold_empleo_ciiu = (
    gold_empleo_ciiu.groupby(
        [
            "anio_censo",
            "sector"
        ],
        dropna=False
    )[
        "personas_ocupadas"
    ]
    .sum()
    .reset_index()
)


gold_empleo_ciiu = (
    gold_empleo_ciiu.sort_values(

        "personas_ocupadas",

        ascending=False
    )
)


guardar_gold(
    gold_empleo_ciiu,
    "gold_empleo_ciiu"
)


# =========================================================
# GUARDAR EL ARCHIVO EXCEL
# =========================================================

writer_excel.close()


# =========================================================
# CERRAR SQLITE
# =========================================================

conexion.close()


# =========================================================
# MENSAJE FINAL
# =========================================================

print()
print("=" * 60)

print(
    "CAPA GOLD CREADA CORRECTAMENTE"
)

print("=" * 60)

print()

print(
    "Archivo Excel creado:"
)

print(
    ARCHIVO_EXCEL
)

print()

print(
    "El archivo se encuentra en:"
)

print(
    CARPETA_EXCEL
)

print()

print(
    "Hojas creadas:"
)

print(
    """
gold_pib_tendencia
gold_petroleo_30dias
gold_iee_tendencia
gold_vab_provincia
gold_vab_provincia_sector
gold_empresas_provincia
gold_bachilleres_provincia
gold_bachilleres_vs_empresas
gold_empleo_tendencia
gold_empleo_ciiu
"""
)