import mysql.connector
from mysql.connector import Error

def connect_to_database(host, user, password, database):
    """
    Establece una conexión con la base de datos y retorna el objeto de conexión y cursor.
    """
    try:
        conn = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            database=database
        )

        if conn.is_connected():
            print("Conexión exitosa a la base de datos.")
            cursor = conn.cursor()
            return conn, cursor
        else:
            print("No se pudo conectar a la base de datos.")
            return None, None

    except Error as e:
        print(f"Error al conectar a la base de datos: {e}")
        return None, None

def execute_query(cursor, query):
    """
    Ejecuta una consulta SQL y retorna el resultado.
    """
    try:
        cursor.execute(query)
        return cursor.fetchone()
    except Error as e:
        print(f"Error al ejecutar la consulta: {e}")
        return None

def close_connection(conn, cursor):
    """
    Cierra el cursor y la conexión a la base de datos.
    """
    if conn.is_connected():
        cursor.close()
        conn.close()
        print("Conexión cerrada.")

def main():
    # Parámetros de conexión
    host = "190.210.132.63" # 192.168.186 190.210.132.63
    user = "ht627842_pepe"
    password = "Paisaje.2024*"
    database = "ht627842_apichess"

    # Conectar a la base de datos
    conn, cursor = connect_to_database(host, user, password, database)

    if conn and cursor:
        # Ejecutar consulta
        db_name = execute_query(cursor, "SELECT DATABASE();")
        if db_name:
            print("Conectado a la base de datos:", db_name[0])

        # Cerrar conexión
        close_connection(conn, cursor)

if __name__ == "__main__":
    main()
