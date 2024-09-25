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

# Función para consultar artículos
def consultar_articulos(session_id, articulo="", nroLote="", anulado="NO"):
    url = f"{base_url}/articulos/"
    headers = {
        "Cookie": session_id,  # Correcto formato del header de autenticación
        "Accept": "application/json"
    }
    
    params = {
        "articulo": articulo,
        "nroLote": nroLote,
        "anulado": anulado
    }
    
    response = requests.get(url, headers=headers, params=params)
    
    if response.status_code == 200:
        try:
            return response.json()
        except json.JSONDecodeError:
            return response.text  # Retorna el texto en caso de que no sea un JSON válido
    else:
        raise Exception("Error en la consulta de artículos:", response.text)

print("termine aqui")

# Función principal
def obtener_todos_lotes_y_guardar(session_id):
    resultados_completos = []
    
    # Consultar el lote 0 para obtener la cantidad total de lotes
    resultado_lote_0 = consultar_articulos(session_id, articulo="", nroLote=0, anulado="NO")
    
    # Imprimir la respuesta completa para diagnóstico
    print("Respuesta del lote 0:", resultado_lote_0)
    
    if isinstance(resultado_lote_0, dict):
        # Extraer el número total de lotes desde la clave 'cantArticulos'
        cant_articulos_str = resultado_lote_0.get('cantArticulos', '')
        num_lotes_str = cant_articulos_str.split('Numero de lote obtenido: ')[-1].split('.')[0]
        num_lotes = int(num_lotes_str.split('/')[1]) if '/' in num_lotes_str else 0
        print(f"Se encontraron {num_lotes} lotes en total.")
    else:
        print("Error al obtener el lote 0, no se puede continuar.")
        return
    
    # Iterar sobre todos los lotes desde 1 hasta el número máximo de lotes
    for lote in range(1, num_lotes + 1):
        try:
            resultado = consultar_articulos(session_id, articulo="", nroLote=lote, anulado="NO")
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
        with open('resultados_articulos.json', 'w', encoding='utf-8') as json_file:
            json.dump(resultados_completos, json_file, ensure_ascii=False, indent=4)
        
        # Guardar resultados en un archivo CSV (opcional)
        keys = resultados_completos[0].keys() if resultados_completos else []
        with open('resultados_articulos.csv', 'w', newline='', encoding='utf-8') as csv_file:
            dict_writer = csv.DictWriter(csv_file, fieldnames=keys)
            dict_writer.writeheader()
            dict_writer.writerows(resultados_completos)
        
        print("Los resultados han sido guardados en 'resultados_articulos.json' y 'resultados_articulos.csv'.")
    else:
        print("No se obtuvieron resultados válidos para guardar.")

# Proceso principal
try:
    # Autenticación
    session_id = autenticar()
    print(f"Autenticación exitosa. Session ID: {session_id}")
    
    # Obtener todos los lotes y guardar en un archivo
    obtener_todos_lotes_y_guardar(session_id)
    
except Exception as e:
    print(str(e))
