import json
import mysql.connector
from mysql.connector import Error

try:
    # Conectar a la base de datos de Telefonica
    conn = mysql.connector.connect(
        host="192.168.187.234",
        user="ht627223_apiche@admin.hostingtelefonica.com.ar",
        password="dPepq.$8#=;yOa7@",
        database="ht627223_api_chess"
    )

    if conn.is_connected():
        print("Conexión exitosa a la base de datos.")
        
        cursor = conn.cursor()

        # Cargar el archivo JSON
        with open('frescura.json', 'r') as file:
            data = json.load(file)

        # Insertar los datos en la base de datos
        for item in data['dsStockFisicoApi']['dsStock']:
            sql = """
            INSERT INTO inventario (fecha, idDeposito, idAlmacen, idArticulo, dsArticulo, fecVtoLote, cantBultos, cantUnidades)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """
            val = (
                item['fecha'],
                item['idDeposito'],
                item['idAlmacen'],
                item['idArticulo'],
                item['dsArticulo'],
                item['fecVtoLote'],
                item['cantBultos'],
                item['cantUnidades']
            )
            cursor.execute(sql, val)

        # Confirmar los cambios
        conn.commit()

        print("Datos insertados con éxito.")

except Error as e:
    print("Error al conectar o interactuar con la base de datos:", e)

finally:
    if conn.is_connected():
        cursor.close()
        conn.close()
        print("Conexión cerrada.")
