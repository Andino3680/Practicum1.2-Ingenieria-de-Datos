import pandas as pd
from pathlib import Path
import unicodedata
import sqlite3

carpeta_datos = Path("reto-macroentorno")
carpeta_salida = Path("salida-csv-limpio")
carpeta_db = Path("base-datos")
carpeta_sql = Path("sql")

carpeta_salida.mkdir(exist_ok=True)
carpeta_db.mkdir(exist_ok=True)
carpeta_sql.mkdir(exist_ok=True)

db_path = carpeta_db / "macroentorno.db"


def limpiar_texto(valor):
    if pd.isna(valor):
        return ""
    texto = str(valor).strip().upper()
    texto = " ".join(texto.split())
    return texto


def quitar_tildes(valor):
    texto = limpiar_texto(valor)
    texto = unicodedata.normalize("NFD", texto)
    texto = "".join(c for c in texto if unicodedata.category(c) != "Mn")
    return texto


def buscar_archivo(claves, extension=None, excluir=None):
    if excluir is None:
        excluir = []

    for archivo in carpeta_datos.iterdir():
        nombre = quitar_tildes(archivo.name)

        if extension is not None and archivo.suffix.lower() != extension.lower():
            continue

        cumple = True

        for clave in claves:
            if quitar_tildes(clave) not in nombre:
                cumple = False

        for palabra in excluir:
            if quitar_tildes(palabra) in nombre:
                cumple = False

        if cumple:
            return archivo

    print("Archivos encontrados en reto-macroentorno:")
    for archivo in carpeta_datos.iterdir():
        print("-", archivo.name)

    raise FileNotFoundError(f"No encontré archivo con claves: {claves}")


def guardar_csv(df, nombre):
    ruta = carpeta_salida / nombre
    df.to_csv(ruta, index=False, encoding="utf-8-sig")
    return ruta


def limpiar_pib_nominal():
    ruta = buscar_archivo(["pib"], extension=".xlsx", excluir=["retropolacion"])

    df = pd.read_excel(ruta, sheet_name=0, header=0)

    df = df.iloc[:, 0:4].copy()
    df.columns = [
        "anio",
        "pib_base_2018_indice",
        "variacion_pib_anual",
        "pib_percapita_nominal_usd"
    ]

    df["anio"] = pd.to_numeric(df["anio"], errors="coerce").astype("Int64")
    df["fecha"] = pd.to_datetime(df["anio"].astype(str) + "-01-01", errors="coerce")

    df["pib_base_2018_indice"] = pd.to_numeric(df["pib_base_2018_indice"], errors="coerce")
    df["variacion_pib_anual"] = pd.to_numeric(df["variacion_pib_anual"], errors="coerce")
    df["pib_percapita_nominal_usd"] = pd.to_numeric(df["pib_percapita_nominal_usd"], errors="coerce")

    if df["variacion_pib_anual"].max() <= 1:
        df["variacion_pib_anual_pct"] = df["variacion_pib_anual"] * 100
    else:
        df["variacion_pib_anual_pct"] = df["variacion_pib_anual"]

    df = df.dropna(subset=["anio"])
    df = df.drop_duplicates()

    df = df[
        [
            "fecha",
            "anio",
            "pib_base_2018_indice",
            "variacion_pib_anual_pct",
            "pib_percapita_nominal_usd"
        ]
    ]

    guardar_csv(df, "silver_pib_nominal.csv")
    return df


def limpiar_pib_real():
    ruta = buscar_archivo(["retropolacion", "pib"], extension=".xlsx")

    pib = pd.read_excel(ruta, sheet_name="PIB pc real", header=9)
    pib = pib.dropna(how="all")

    pib = pib[
        [
            "Años",
            "PIB \n(Millones de USD encadenado de volumen)",
            "Población",
            "PIB Per cápita  \n(USD)"
        ]
    ].copy()

    pib.columns = [
        "anio",
        "pib_real_musd",
        "poblacion",
        "pib_percapita_real_usd"
    ]

    variacion = pd.read_excel(ruta, sheet_name="Variación anual Gasto", header=9)
    variacion = variacion[
        [
            "Años",
            "Producto Interno Bruto"
        ]
    ].copy()

    variacion.columns = [
        "anio",
        "variacion_pib_pct"
    ]

    df = pd.merge(pib, variacion, on="anio", how="left")

    df["anio"] = pd.to_numeric(df["anio"], errors="coerce").astype("Int64")
    df["pib_real_musd"] = pd.to_numeric(df["pib_real_musd"], errors="coerce")
    df["poblacion"] = pd.to_numeric(df["poblacion"], errors="coerce").astype("Int64")
    df["pib_percapita_real_usd"] = pd.to_numeric(df["pib_percapita_real_usd"], errors="coerce")
    df["variacion_pib_pct"] = pd.to_numeric(df["variacion_pib_pct"], errors="coerce")

    df = df.dropna(subset=["anio", "pib_real_musd"])
    df = df.drop_duplicates()
    df = df.sort_values("anio")

    guardar_csv(df, "silver_pib_real.csv")
    return df


def limpiar_petroleo():
    ruta = buscar_archivo(["petr"], extension=".xlsx")

    df = pd.read_excel(ruta, sheet_name=0, header=1)
    df = df.iloc[:, 0:2].copy()
    df.columns = ["fecha", "precio_petroleo_wti"]

    df["fecha"] = pd.to_datetime(df["fecha"], errors="coerce")
    df["precio_petroleo_wti"] = pd.to_numeric(df["precio_petroleo_wti"], errors="coerce")

    df = df.dropna(subset=["fecha", "precio_petroleo_wti"])
    df = df.drop_duplicates()
    df = df.sort_values("fecha")

    guardar_csv(df, "silver_petroleo.csv")
    return df


def limpiar_riesgo_pais():
    ruta = buscar_archivo(["riesgo"], extension=".xlsx")

    df = pd.read_excel(ruta, sheet_name=0, header=1)
    df = df.iloc[:, 0:2].copy()
    df.columns = ["fecha", "riesgo_pais_pb"]

    df["fecha"] = pd.to_datetime(df["fecha"], errors="coerce")
    df["riesgo_pais_pb"] = pd.to_numeric(df["riesgo_pais_pb"], errors="coerce").astype("Int64")

    df = df.dropna(subset=["fecha", "riesgo_pais_pb"])
    df = df.drop_duplicates()
    df = df.sort_values("fecha")

    guardar_csv(df, "silver_riesgo_pais.csv")
    return df


def limpiar_iee():
    ruta = buscar_archivo(["iee"], extension=".xlsx")

    df = pd.read_excel(ruta, sheet_name="IEE", header=7)

    df = df.iloc[:, 0:6].copy()
    df.columns = [
        "fecha",
        "iee_global",
        "comercio",
        "construccion",
        "manufactura",
        "servicios"
    ]

    df["fecha"] = pd.to_datetime(df["fecha"], errors="coerce")

    for col in ["iee_global", "comercio", "construccion", "manufactura", "servicios"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df = df.dropna(subset=["fecha", "iee_global"])
    df = df.drop_duplicates()
    df = df.sort_values("fecha")

    guardar_csv(df, "silver_iee.csv")
    return df


def limpiar_vab():
    ruta = buscar_archivo(["vab", "2018"], extension=".xlsx")

    df = pd.read_excel(ruta, sheet_name="DATA", header=0)

    df = df.rename(columns={
        "AÑO": "anio",
        "CÓDIGO PROVINCIA": "cod_provincia",
        "PROVINCIA": "provincia",
        "CÓDIGO CANTÓN": "cod_canton",
        "CANTÓN": "canton",
        "SECTOR": "sector",
        "VALOR": "vab_usd"
    })

    for col in ["provincia", "canton", "sector"]:
        df[col] = df[col].apply(limpiar_texto)

    df["provincia_normalizada"] = df["provincia"].apply(quitar_tildes)
    df["canton_normalizado"] = df["canton"].apply(quitar_tildes)

    df["anio"] = pd.to_numeric(df["anio"], errors="coerce").astype("Int64")
    df["cod_provincia"] = pd.to_numeric(df["cod_provincia"], errors="coerce").astype("Int64")
    df["cod_canton"] = pd.to_numeric(df["cod_canton"], errors="coerce").astype("Int64")
    df["vab_usd"] = pd.to_numeric(df["vab_usd"], errors="coerce")

    df = df.dropna(subset=["anio", "provincia", "canton", "sector", "vab_usd"])
    df = df.drop_duplicates()

    guardar_csv(df, "silver_vab.csv")
    return df


def limpiar_mineduc():
    try:
        ruta = buscar_archivo(["mineduc", "inicio"], extension=".csv")
    except:
        ruta = buscar_archivo(["inicio"], extension=".csv")

    df = pd.read_csv(
        ruta,
        sep=";",
        encoding="latin-1",
        low_memory=False
    )

    df.columns = df.columns.str.strip()

    columnas = {
        "Año lectivo": "anio_lectivo",
        "AMIE": "amie",
        "Nombre_Institución": "nombre_institucion",
        "Provincia": "provincia",
        "Cod_Provincia": "cod_provincia",
        "Cantón": "canton",
        "Cod_Cantón": "cod_canton",
        "Nivel Educación": "nivel_educacion",
        "Sostenimiento": "sostenimiento",
        "Total Estudiantes": "total_estudiantes",
        "EstudiantesMasculinoTercerAñoBACH": "bach_3_masculino",
        "EstudiantesFemeninoTercerAñoBACH": "bach_3_femenino"
    }

    df = df[list(columnas.keys())].rename(columns=columnas)

    for col in [
        "anio_lectivo",
        "amie",
        "nombre_institucion",
        "provincia",
        "canton",
        "nivel_educacion",
        "sostenimiento"
    ]:
        df[col] = df[col].apply(limpiar_texto)

    df["provincia_normalizada"] = df["provincia"].apply(quitar_tildes)
    df["canton_normalizado"] = df["canton"].apply(quitar_tildes)

    for col in [
        "cod_provincia",
        "cod_canton",
        "total_estudiantes",
        "bach_3_masculino",
        "bach_3_femenino"
    ]:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)

    df["bachilleres_3ro"] = df["bach_3_masculino"] + df["bach_3_femenino"]

    df["nivel_normalizado"] = df["nivel_educacion"].apply(quitar_tildes)
    df = df[df["nivel_normalizado"].str.contains("BACHILLERATO", na=False)]
    df = df[df["bachilleres_3ro"] > 0]
    df = df.drop(columns=["nivel_normalizado"])
    df = df.drop_duplicates()

    guardar_csv(df, "silver_mineduc_bachillerato.csv")
    return df


def crear_gold_pib(pib_real):
    df = pib_real.copy()

    def clasificar(valor):
        if pd.isna(valor):
            return "Sin dato"
        if valor > 2:
            return "Crecimiento fuerte"
        if valor > 0:
            return "Crecimiento moderado"
        if valor == 0:
            return "Estancamiento"
        return "Contracción"

    df["clasificacion"] = df["variacion_pib_pct"].apply(clasificar)
    guardar_csv(df, "gold_pib_tendencia.csv")
    return df


def crear_gold_petroleo(petroleo, riesgo):
    df = pd.merge(petroleo, riesgo, on="fecha", how="outer")
    df = df.sort_values("fecha")

    df["promedio_wti_30dias"] = (
        df["precio_petroleo_wti"]
        .rolling(window=30, min_periods=1)
        .mean()
    )

    guardar_csv(df, "gold_petroleo_30dias.csv")
    return df


def crear_gold_vab(vab):
    gold = vab.groupby(
        ["anio", "provincia_normalizada", "provincia", "sector"],
        as_index=False
    ).agg(
        vab_total_usd=("vab_usd", "sum")
    )

    gold = gold.sort_values(["anio", "vab_total_usd"], ascending=[False, False])

    guardar_csv(gold, "gold_vab_provincia_sector.csv")
    return gold


def crear_gold_bachilleres(mineduc):
    gold = mineduc.groupby(
        ["provincia_normalizada", "provincia"],
        as_index=False
    ).agg(
        total_bachilleres_3ro=("bachilleres_3ro", "sum"),
        instituciones_bachillerato=("amie", "nunique"),
        total_estudiantes=("total_estudiantes", "sum")
    )

    gold = gold.sort_values("total_bachilleres_3ro", ascending=False)

    guardar_csv(gold, "gold_bachilleres_provincia.csv")
    return gold


def cargar_sqlite(tablas):
    conexion = sqlite3.connect(db_path)

    for nombre, df in tablas.items():
        df.to_sql(nombre, conexion, if_exists="replace", index=False)

    conexion.close()


if __name__ == "__main__":
    print("Iniciando pipeline 4to ciclo...")

    silver_pib_nominal = limpiar_pib_nominal()
    silver_pib_real = limpiar_pib_real()
    silver_petroleo = limpiar_petroleo()
    silver_riesgo_pais = limpiar_riesgo_pais()
    silver_iee = limpiar_iee()
    silver_vab = limpiar_vab()
    silver_mineduc = limpiar_mineduc()

    gold_pib = crear_gold_pib(silver_pib_real)
    gold_petroleo = crear_gold_petroleo(silver_petroleo, silver_riesgo_pais)
    gold_vab = crear_gold_vab(silver_vab)
    gold_bachilleres = crear_gold_bachilleres(silver_mineduc)

    tablas_sqlite = {
        "silver_pib_nominal": silver_pib_nominal,
        "silver_pib_real": silver_pib_real,
        "silver_petroleo": silver_petroleo,
        "silver_riesgo_pais": silver_riesgo_pais,
        "silver_iee": silver_iee,
        "silver_vab": silver_vab,
        "silver_mineduc_bachillerato": silver_mineduc,
        "gold_pib_tendencia": gold_pib,
        "gold_petroleo_30dias": gold_petroleo,
        "gold_vab_provincia_sector": gold_vab,
        "gold_bachilleres_provincia": gold_bachilleres
    }

    cargar_sqlite(tablas_sqlite)

    print()
    print("Pipeline terminado correctamente.")
    print("CSV limpios creados en: salida-csv-limpio")
    print("Base SQLite creada en: base-datos/macroentorno_4to.db")
    print()
    print("Filas generadas:")
    print("silver_pib_nominal:", len(silver_pib_nominal))
    print("silver_pib_real:", len(silver_pib_real))
    print("silver_petroleo:", len(silver_petroleo))
    print("silver_riesgo_pais:", len(silver_riesgo_pais))
    print("silver_iee:", len(silver_iee))
    print("silver_vab:", len(silver_vab))
    print("silver_mineduc_bachillerato:", len(silver_mineduc))
    print("gold_bachilleres_provincia:", len(gold_bachilleres))
    print()
    print(gold_bachilleres.head(10))