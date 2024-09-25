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
        "Cookie": f"{session_id}",
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
    print("Respuesta del lote 0:", json.dumps(resultado_lote_0, indent=4, ensure_ascii=False))
    
    if isinstance(resultado_lote_0, dict):
        # Extraer el número total de lotes desde 'cantComprobantesVentas'
        cant_comprobantes_ventas = resultado_lote_0.get('cantComprobantesVentas', '')
        if 'Numero de lote obtenido:' in cant_comprobantes_ventas:
            # Extraer la parte '1/1' del texto
            lote_info = cant_comprobantes_ventas.split('Numero de lote obtenido:')[-1].split('.')[0].strip()
            lote_actual, total_lotes = lote_info.split('/')
            num_lotes = int(total_lotes)
            print(f"Se encontraron {num_lotes} lotes en total.")
        else:
            print("No se pudo extraer el número de lotes de 'cantComprobantesVentas'.")
            return
    else:
        print("Error al obtener el lote 0, no se puede continuar.")
        return

    # Iterar sobre todos los lotes desde 1 hasta el número máximo de lotes
    for lote in range(1, num_lotes + 1):
        try:
            resultado = consultar_ventas(session_id, fechaDesde, fechaHasta, nroLote=lote)
            print(f"Lote {lote}: Resultado obtenido (tipo {type(resultado)}): {json.dumps(resultado, indent=4, ensure_ascii=False)}")
            
            # Verificar si hay datos en 'dsReporteComprobantesApi' ➔ 'VentasResumen'
            if isinstance(resultado, dict) and 'dsReporteComprobantesApi' in resultado:
                reporte = resultado['dsReporteComprobantesApi']
                if 'VentasResumen' in reporte:
                    ventas_resumen = reporte['VentasResumen']
                    if isinstance(ventas_resumen, list):
                        resultados_completos.extend(ventas_resumen)
                    else:
                        print(f"Lote {lote}: 'VentasResumen' no es una lista.")
                else:
                    print(f"Lote {lote}: Clave 'VentasResumen' no encontrada en 'dsReporteComprobantesApi'.")
            else:
                print(f"Lote {lote}: Resultado inesperado, no se agregará a la lista.")
                if isinstance(resultado, dict):
                    print(f"Lote {lote}: Claves disponibles en el resultado: {list(resultado.keys())}")
                else:
                    print(f"Lote {lote}: El resultado no es un diccionario.")
                
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
    fechaHasta = "2024/06/01"
    
    # Obtener todas las ventas y guardar en un archivo
    obtener_todas_ventas_y_guardar(session_id, fechaDesde, fechaHasta)
    
except Exception as e:
    print(str(e))