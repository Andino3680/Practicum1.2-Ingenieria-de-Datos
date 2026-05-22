import sqlite3
import pandas as pd # type: ignore
import os
import unicodedata

# =========================
# RUTAS DEL PROYECTO
# =========================

RUTA_DB = "db/amie_mineduc.db"
RUTA_CSV_LIMPIO = "salida/amie_limpio.csv"
RUTA_DIAGNOSTICO = "salida/diagnostico_limpieza.txt"

os.makedirs("salida", exist_ok=True)


# =========================
# FUNCIONES AUXILIARES
# =========================

def limpiar_nombre_columna(nombre):
    nombre = str(nombre).strip().lower()

    # Quitar tildes
    nombre = unicodedata.normalize("NFKD", nombre)
    nombre = "".join([c for c in nombre if not unicodedata.combining(c)])

    # Reemplazar espacios y símbolos
    nombre = nombre.replace(" ", "_")
    nombre = nombre.replace("-", "_")
    nombre = nombre.replace("/", "_")
    nombre = nombre.replace(".", "")
    nombre = nombre.replace("(", "")
    nombre = nombre.replace(")", "")
    nombre = nombre.replace("__", "_")

    return nombre


def obtener_tablas(conexion):
    consulta = """
    SELECT name 
    FROM sqlite_master 
    WHERE type = 'table';
    """
    tablas = pd.read_sql_query(consulta, conexion)
    return tablas["name"].tolist()


def convertir_a_entero(df, columna):
    if columna in df.columns:
        df[columna] = pd.to_numeric(df[columna], errors="coerce")
        df[columna] = df[columna].fillna(0).astype(int)


# =========================
# PROCESO PRINCIPAL
# =========================

def main():
    conexion = sqlite3.connect(RUTA_DB)

    tablas = obtener_tablas(conexion)

    print("Tablas encontradas:")
    for tabla in tablas:
        print("-", tabla)

    dataframes = []

    for tabla in tablas:
        nombre_tabla = tabla.lower()

        # Evitar leer diccionarios o la tabla limpia anterior
        if "diccionario" not in nombre_tabla and "instituciones_limpias" not in nombre_tabla:
            print("Leyendo tabla:", tabla)

            consulta = f'SELECT * FROM "{tabla}"'
            df = pd.read_sql_query(consulta, conexion)

            # Limpiar nombres de columnas
            df.columns = [limpiar_nombre_columna(col) for col in df.columns]

            dataframes.append(df)

    if len(dataframes) == 0:
        print("No se encontraron tablas válidas para limpiar.")
        conexion.close()
        return

    # Unir todas las tablas importadas
    df_total = pd.concat(dataframes, ignore_index=True)

    print("Filas antes de limpiar:", len(df_total))
    print("Columnas encontradas:")
    print(df_total.columns.tolist())

    # =========================
    # RENOMBRADO DE COLUMNAS
    # =========================

    cambios = {
        "ano_lectivo": "anio_lectivo",
        "ao_lectivo": "anio_lectivo",
        "amie": "cod_amie",
        "nombre_institucion": "nombre_institucion",
        "provincia": "provincia",
        "canton": "canton",
        "parroquia": "parroquia",
        "sostenimiento": "sostenimiento",
        "area": "area",
        "nivel_educacion": "nivel_educacion",
        "modalidad": "modalidad",
        "jornada": "jornada",
        "zona": "zona",
        "total_estudiantes": "total_estudiantes",
        "total_docentes": "total_docentes",
        "estudiantes_femenino": "estudiantes_f",
        "estudiantes_masculino": "estudiantes_m",
        "docentes_femenino": "docentes_f",
        "docentes_masculino": "docentes_m"
    }

    df_total = df_total.rename(columns=cambios)

    # =========================
    # LIMPIEZA DE TEXTOS
    # =========================

    columnas_texto = [
        "anio_lectivo",
        "cod_amie",
        "nombre_institucion",
        "provincia",
        "canton",
        "parroquia",
        "sostenimiento",
        "area",
        "nivel_educacion",
        "modalidad",
        "jornada",
        "zona"
    ]

    for columna in columnas_texto:
        if columna in df_total.columns:
            df_total[columna] = df_total[columna].astype(str).str.strip()

    # =========================
    # LIMPIEZA DE NULOS NUMÉRICOS
    # =========================

    columnas_numericas = [
        "total_estudiantes",
        "total_docentes",
        "estudiantes_f",
        "estudiantes_m",
        "docentes_f",
        "docentes_m"
    ]

    for columna in columnas_numericas:
        convertir_a_entero(df_total, columna)

    # =========================
    # ELIMINACIÓN DE DUPLICADOS
    # =========================

    filas_antes_duplicados = len(df_total)

    if "cod_amie" in df_total.columns and "anio_lectivo" in df_total.columns:
        df_total = df_total.drop_duplicates(subset=["cod_amie", "anio_lectivo"])

    filas_despues_duplicados = len(df_total)
    duplicados_eliminados = filas_antes_duplicados - filas_despues_duplicados

    # =========================
    # VALIDACIÓN DE CONSISTENCIA
    # =========================

    inconsistentes = pd.DataFrame()

    if (
        "total_estudiantes" in df_total.columns
        and "estudiantes_f" in df_total.columns
        and "estudiantes_m" in df_total.columns
    ):
        df_total["diferencia_estudiantes"] = df_total["total_estudiantes"] - (
            df_total["estudiantes_f"] + df_total["estudiantes_m"]
        )

        inconsistentes = df_total[df_total["diferencia_estudiantes"] != 0]

    print("Filas después de limpiar:", len(df_total))

    # =========================
    # CREAR DIAGNÓSTICO
    # =========================

    diagnostico = []

    diagnostico.append("DIAGNÓSTICO DEL DATASET AMIE - MINEDUC\n")
    diagnostico.append("======================================\n\n")

    diagnostico.append(f"Total de filas antes de limpiar: {filas_antes_duplicados}\n")
    diagnostico.append(f"Total de filas después de limpiar: {len(df_total)}\n")
    diagnostico.append(f"Duplicados eliminados: {duplicados_eliminados}\n")
    diagnostico.append(f"Total de columnas: {len(df_total.columns)}\n\n")

    diagnostico.append("COLUMNAS DISPONIBLES:\n")
    for col in df_total.columns:
        diagnostico.append(f"- {col}\n")

    diagnostico.append("\nNULOS POR COLUMNA:\n")
    diagnostico.append(str(df_total.isnull().sum()))
    diagnostico.append("\n\n")

    if "provincia" in df_total.columns:
        diagnostico.append("REGISTROS POR PROVINCIA:\n")
        diagnostico.append(str(df_total["provincia"].value_counts()))
        diagnostico.append("\n\n")

    if "anio_lectivo" in df_total.columns:
        diagnostico.append("REGISTROS POR AÑO LECTIVO:\n")
        diagnostico.append(str(df_total["anio_lectivo"].value_counts()))
        diagnostico.append("\n\n")

    if len(inconsistentes) > 0:
        diagnostico.append("VALIDACIÓN DE CONSISTENCIA:\n")
        diagnostico.append(
            f"Registros donde total_estudiantes no coincide con estudiantes_f + estudiantes_m: {len(inconsistentes)}\n"
        )
    else:
        diagnostico.append("VALIDACIÓN DE CONSISTENCIA:\n")
        diagnostico.append("No se encontraron inconsistencias en matrícula.\n")

    with open(RUTA_DIAGNOSTICO, "w", encoding="utf-8") as archivo:
        archivo.writelines(diagnostico)

    # =========================
    # GUARDAR RESULTADOS
    # =========================

    df_total.to_csv(RUTA_CSV_LIMPIO, index=False, encoding="utf-8-sig")

    df_total.to_sql(
        "instituciones_limpias",
        conexion,
        if_exists="replace",
        index=False
    )

    conexion.close()

    print("Proceso terminado.")
    print("CSV limpio guardado en:", RUTA_CSV_LIMPIO)
    print("Diagnóstico guardado en:", RUTA_DIAGNOSTICO)
    print("Tabla creada en SQLite: instituciones_limpias")


if __name__ == "__main__":
    main()