import requests
import json
import csv

# Definimos la URL base de la API y la URL de autenticación
base_url = "http://179.51.237.14:8081/web/api/chess/v1"
auth_url = f"{base_url}/auth/login"

# Credenciales de usuario
credenciales = {
    "usuario": "delpalacioapi",
    "password": "1234"
}

# Función para autenticar y obtener el sessionId
def autenticar():
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json"
    }

    response = requests.post(auth_url, json=credenciales, headers=headers)
    
    if response.status_code == 200:
        session_id = response.json().get('sessionId')
        return session_id
    else:
        raise Exception("Error en la autenticación:", response.text)

# Función para consultar ventas
def consultar_ventas(session_id, fechaDesde, fechaHasta, empresas="", detallado="yes", nroLote=""):
    url = f"{base_url}/ventas/"
    headers = {
        "Cookie": session_id,
        "Accept": "application/json"
    }
    
    params = {
        "fechaDesde": fechaDesde,
        "fechaHasta": fechaHasta,
        "empresas": empresas,
        "detallado": detallado,
        "nroLote": nroLote
    }
    
    response = requests.get(url, headers=headers, params=params)
    
    if response.status_code == 200:
        try:
            return response.json()
        except json.JSONDecodeError:
            return response.text  # Retorna el texto en caso de que no sea un JSON válido
    else:
        raise Exception("Error en la consulta de ventas:", response.text)

# Función principal para obtener todas las ventas y guardarlas
def obtener_todas_ventas_y_guardar(session_id, fechaDesde, fechaHasta):
    resultados_completos = []
    
    # Consultar el lote 0 para obtener la cantidad total de lotes
    resultado_lote_0 = consultar_ventas(session_id, fechaDesde, fechaHasta, nroLote=0)
    
    # Imprimir la respuesta completa para diagnóstico
    print("Respuesta del lote 0:", resultado_lote_0)
    
    if isinstance(resultado_lote_0, dict):
        # Dependiendo del formato de respuesta, ajusta esta parte para extraer el número total de lotes
        # Por ejemplo, si hay una clave 'cantVentas' o 'totalLotes'
        num_lotes = resultado_lote_0.get('totalLotes', 0)
        if not num_lotes:
            # Si no existe 'totalLotes', intenta extraerlo de otra forma
            mensaje = resultado_lote_0.get('mensaje', '')
            if 'Numero de lote obtenido' in mensaje:
                num_lotes_str = mensaje.split('Numero de lote obtenido: ')[-1].split('/')[1].split('.')[0]
                num_lotes = int(num_lotes_str)
        print(f"Se encontraron {num_lotes} lotes en total.")
    else:
        print("Error al obtener el lote 0, no se puede continuar.")
        return
    
    # Iterar sobre todos los lotes desde 1 hasta el número máximo de lotes
    for lote in range(1, num_lotes + 1):
        try:
            resultado = consultar_ventas(session_id, fechaDesde, fechaHasta, nroLote=lote)
            print(f"Lote {lote}: Resultado obtenido:", resultado)
            
            # Solo agregar resultados válidos
            if isinstance(resultado, list):
                resultados_completos.extend(resultado)
            elif isinstance(resultado, dict):
                resultados_completos.append(resultado)
            else:
                print(f"Lote {lote}: Resultado inesperado, no se agregará a la lista.")
            
        except Exception as e:
            print(f"Error en la consulta para lote {lote}: {str(e)}")

    # Verifica si hay resultados antes de intentar guardar
    if resultados_completos:
        # Guardar resultados en un archivo JSON
        with open('resultados_ventas.json', 'w', encoding='utf-8') as json_file:
            json.dump(resultados_completos, json_file, ensure_ascii=False, indent=4)
        
        # Guardar resultados en un archivo CSV (opcional)
        keys = resultados_completos[0].keys() if resultados_completos else []
        with open('resultados_ventas.csv', 'w', newline='', encoding='utf-8') as csv_file:
            dict_writer = csv.DictWriter(csv_file, fieldnames=keys)
            dict_writer.writeheader()
            dict_writer.writerows(resultados_completos)
        
        print("Los resultados han sido guardados en 'resultados_ventas.json' y 'resultados_ventas.csv'.")
    else:
        print("No se obtuvieron resultados válidos para guardar.")

# Proceso principal
try:
    # Autenticación
    session_id = autenticar()
    print(f"Autenticación exitosa. Session ID: {session_id}")
    
    # Definir las fechas
    fechaDesde = "2024/06/01"
    fechaHasta = "2024/06/30"
    
    # Obtener todas las ventas y guardar en un archivo
    obtener_todas_ventas_y_guardar(session_id, fechaDesde, fechaHasta)
    
except Exception as e:
    print(str(e))
