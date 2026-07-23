import json
import re
import sqlite3
from pathlib import Path

import oracledb
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

RUTA_CSV = (
    PROYECTO
    / "salida-csv-limpio"
)

RUTA_CSV.mkdir(
    parents=True,
    exist_ok=True
)


# =========================================================
# INDICADORES DE ORACLE -> TABLAS SILVER
# =========================================================

TABLAS = {
    "BCE_IEE_GLOBAL":
        "silver_iee",

    "CENSO_GRUPO_OCUPACION":
        "silver_censo_grupo_ocupacion",

    "CENSO_RAMA_ACTIVIDAD":
        "silver_censo_rama_actividad",

    "INEC_ENEMDU_POBLACIONES":
        "silver_enemdu",

    "MATRIZ_EMPLEO_TOTAL":
        "silver_empleo_total",

    "MATRIZ_EMPLEO_VAB":
        "silver_empleo_vab",

    "MINEDUC_AMIE_COSTA":
        "silver_mineduc",

    "PIB_NOMINAL_PER_CAPITA":
        "silver_pib_nominal",

    "PIB_REAL_PER_CAPITA":
        "silver_pib_real",

    "PRECIO_PETROLEO_WTI":
        "silver_petroleo",

    "RIESGO_PAIS":
        "silver_riesgo_pais",

    "SUPERCIAS_DIRECTORIO":
        "silver_supercias_directorio",

    "SUPERCIAS_RANKING":
        "silver_supercias_ranking",

    "VAB_CANTONAL_CIIU":
        "silver_vab"
}


# =========================================================
# LIMPIAR NOMBRES DE COLUMNAS
# =========================================================

def limpiar_nombre(nombre):

    nombre = str(nombre).strip().lower()

    reemplazos = {
        "á": "a",
        "é": "e",
        "í": "i",
        "ó": "o",
        "ú": "u",
        "ü": "u",
        "ñ": "n"
    }

    for original, nuevo in reemplazos.items():
        nombre = nombre.replace(
            original,
            nuevo
        )

    nombre = re.sub(
        r"[^a-z0-9_]+",
        "_",
        nombre
    )

    nombre = re.sub(
        r"_+",
        "_",
        nombre
    )

    return nombre.strip("_")


# =========================================================
# LEER JSON DEL RPA
# =========================================================

def leer_json(datos_json):

    if datos_json is None:
        return None

    # Oracle puede devolver CLOB
    if hasattr(datos_json, "read"):
        texto_json = datos_json.read()
    else:
        texto_json = datos_json

    # Primer intento de lectura
    datos = json.loads(texto_json)

    # Algunos registros del RPA tienen
    # el JSON codificado dos veces
    if isinstance(datos, str):
        datos = json.loads(datos)

    return datos


# =========================================================
# PREPARAR VALORES PARA SQLITE
# =========================================================

def preparar_valor(valor):

    # SQLite no guarda directamente
    # listas ni diccionarios
    if isinstance(
        valor,
        (list, dict)
    ):
        return json.dumps(
            valor,
            ensure_ascii=False
        )

    return valor


# =========================================================
# CONEXIÓN A ORACLE
# =========================================================

print("Conectando con Oracle...")

conexion_oracle = oracledb.connect(
    user="RPA",
    password="rpapwdhr",
    dsn="localhost:1521/XEPDB1"
)

cursor = conexion_oracle.cursor()

print("Conexión con Oracle correcta.")


# =========================================================
# CONEXIÓN A SQLITE
# =========================================================

conexion_sqlite = sqlite3.connect(
    RUTA_DB
)

print(
    "Base SQLite:",
    RUTA_DB
)


# =========================================================
# PROCESAR TODOS LOS INDICADORES
# =========================================================

for indicador, tabla_silver in TABLAS.items():

    print()
    print("=" * 60)
    print(
        "Procesando:",
        indicador
    )

    consulta = """
        SELECT
            ID,
            DATO_CLAVE,
            FECHA_EXTRACCION,
            DATOS_JSON
        FROM TAB_CONSOLIDADO
        WHERE INDICADOR = :indicador
    """

    cursor.execute(
        consulta,
        indicador=indicador
    )

    registros = []

    errores_json = 0

    filas_oracle = 0

    for (
        id_rpa,
        dato_clave,
        fecha_extraccion,
        datos_json
    ) in cursor:

        filas_oracle += 1

        try:

            datos = leer_json(
                datos_json
            )

            # -----------------------------------------
            # JSON que contiene una lista
            # -----------------------------------------

            if isinstance(
                datos,
                list
            ):

                for elemento in datos:

                    if not isinstance(
                        elemento,
                        dict
                    ):
                        continue

                    elemento[
                        "id_rpa"
                    ] = id_rpa

                    elemento[
                        "dato_clave"
                    ] = dato_clave

                    elemento[
                        "fecha_extraccion"
                    ] = fecha_extraccion

                    registros.append(
                        elemento
                    )

            # -----------------------------------------
            # JSON normal tipo diccionario
            # -----------------------------------------

            elif isinstance(
                datos,
                dict
            ):

                datos[
                    "id_rpa"
                ] = id_rpa

                datos[
                    "dato_clave"
                ] = dato_clave

                datos[
                    "fecha_extraccion"
                ] = fecha_extraccion

                registros.append(
                    datos
                )

            else:

                errores_json += 1

        except (
            json.JSONDecodeError,
            TypeError,
            AttributeError,
            ValueError
        ):

            errores_json += 1


    print(
        "Filas encontradas en Oracle:",
        filas_oracle
    )

    print(
        "JSON con errores:",
        errores_json
    )


    # =====================================================
    # SI NO HAY DATOS VÁLIDOS
    # =====================================================

    if not registros:

        print(
            "No se encontraron "
            "registros válidos."
        )

        continue


    # =====================================================
    # CONVERTIR JSON A DATAFRAME
    # =====================================================

    df = pd.json_normalize(
        registros
    )


    # =====================================================
    # CONVERTIR LISTAS Y DICCIONARIOS INTERNOS
    # =====================================================

    for columna in df.columns:

        df[columna] = df[
            columna
        ].apply(
            preparar_valor
        )


    # =====================================================
    # LIMPIAR NOMBRES DE COLUMNAS
    # =====================================================

    df.columns = [
        limpiar_nombre(
            columna
        )
        for columna in df.columns
    ]


    # =====================================================
    # ELIMINAR COLUMNAS TOTALMENTE VACÍAS
    # =====================================================

    df = df.dropna(
        axis=1,
        how="all"
    )


    # =====================================================
    # ELIMINAR DUPLICADOS EXACTOS
    # =====================================================

    filas_antes = len(
        df
    )

    df = df.drop_duplicates()

    filas_despues = len(
        df
    )

    duplicados = (
        filas_antes
        - filas_despues
    )


    # =====================================================
    # GUARDAR TABLA SILVER EN SQLITE
    # =====================================================

    df.to_sql(
        tabla_silver,
        conexion_sqlite,
        if_exists="replace",
        index=False
    )


    # =====================================================
    # GUARDAR COPIA CSV
    # =====================================================

    ruta_archivo_csv = (
        RUTA_CSV
        / f"{tabla_silver}.csv"
    )

    df.to_csv(
        ruta_archivo_csv,
        index=False,
        encoding="utf-8-sig"
    )


    # =====================================================
    # RESULTADO
    # =====================================================

    print(
        "Tabla creada:",
        tabla_silver
    )

    print(
        "Registros generados:",
        filas_antes
    )

    print(
        "Duplicados eliminados:",
        duplicados
    )

    print(
        "Registros finales:",
        filas_despues
    )

    print(
        "Columnas:",
        len(df.columns)
    )


# =========================================================
# CERRAR CONEXIONES
# =========================================================

cursor.close()

conexion_oracle.close()

conexion_sqlite.close()


# =========================================================
# MENSAJE FINAL
# =========================================================

print()
print("=" * 60)

print(
    "PROCESO COMPLETADO"
)

print(
    "Se terminó el procesamiento "
    "de los datos del RPA."
)

print(
    "Base de datos:",
    RUTA_DB
)