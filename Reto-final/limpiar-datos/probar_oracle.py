import oracledb

try:
    conexion = oracledb.connect(
        user="RPA",
        password="rpapwdhr",
        dsn="localhost:1521/XEPDB1"
    )

    cursor = conexion.cursor()

    cursor.execute("SELECT COUNT(*) FROM TAB_CONSOLIDADO")
    cantidad = cursor.fetchone()[0]

    print("Conexión correcta con Oracle")
    print("Registros encontrados:", cantidad)

    cursor.close()
    conexion.close()

except oracledb.Error as error:
    print("Error al conectar con Oracle:")
    print(error)