import mysql.connector
from mysql.connector import Error

try:
    # Establecer la conexión con la base de datos
    connection = mysql.connector.connect(
        host='190.210.132.63',
        database='ht627842_apichess',
        user='ht627842_admin',
        password='Paisaje.2024*'
    )

    if connection.is_connected():
        db_info = connection.get_server_info()
        print(f"Conectado a MySQL Server versión {db_info}")
        cursor = connection.cursor()
        cursor.execute("SELECT DATABASE();")
        record = cursor.fetchone()
        print(f"Conectado a la base de datos: {record}")

except Error as e:
    print(f"Error al conectar a MySQL: {e}")
finally:
    if connection.is_connected():
        cursor.close()
        connection.close()
        print("Conexión a MySQL cerrada")
