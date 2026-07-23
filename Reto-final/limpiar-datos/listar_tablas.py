import sqlite3

conexion = sqlite3.connect("base-datos/macroentorno.db")

cursor = conexion.cursor()

cursor.execute("""
    SELECT name
    FROM sqlite_master
    WHERE type = 'table'
    ORDER BY name
""")

tablas = cursor.fetchall()

print("TABLAS EN MACROENTORNO.DB:")
print()

for tabla in tablas:
    print(tabla[0])

print()
print("Total de tablas:", len(tablas))

conexion.close()