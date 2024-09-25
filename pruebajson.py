import json
import mysql.connector
import time

try:
    # Abrir el archivo JSON con codificación UTF-8
    with open('c:/Users/horac/Desktop/Proyectos WEB/APIdelpalacio/resultados_ventas.json', 'r', encoding='utf-8') as file:
        data = json.load(file)

    # Inspeccionar la estructura de data
    print("Estructura del JSON cargado:")
    print(type(data))  # Esto te indicará si es un dict o list
    print(data)  # Imprime parte del contenido para ver la estructura

    # Verificar si existe la clave 'dsReporteComprobantesApi'
    if 'dsReporteComprobantesApi' in data:
        if 'VentasResumen' in data['dsReporteComprobantesApi']:
            ventas_resumen = data['dsReporteComprobantesApi']['VentasResumen']
            print("Ventas Resumen cargado con éxito")
        else:
            print("La clave 'VentasResumen' no se encuentra en 'dsReporteComprobantesApi'.")
    else:
        print("La clave 'dsReporteComprobantesApi' no se encuentra en el archivo JSON.")
    
    # Continuar con el procesamiento de los datos si todo está bien...

except UnicodeDecodeError as e:
    print(f"Error de decodificación: {e}")

except FileNotFoundError as e:
    print(f"Archivo no encontrado: {e}")

except Exception as e:
    print(f"Error inesperado: {e}")
