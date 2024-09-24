import json
import csv

def json_a_csv(archivo_json, archivo_csv):
    # Leer el archivo JSON
    with open('resultados_articulos.json', 'r', encoding='utf-8') as f:
        datos = json.load(f)
    
    # Extraer la lista de artículos
    articulos = datos.get('Articulos', {}).get('eArticulos', [])

    # Abrir/crear el archivo CSV
    with open(archivo_csv, 'w', newline='', encoding='utf-8') as csvfile:
        escritor_csv = csv.writer(csvfile)

        # Escribir encabezados del CSV
        encabezados = [
            'idArticulo', 'desArticulo', 'unidadesBulto', 'anulado', 'fechaAlta', 'factorVenta', 'minimoVenta',
            'pesable', 'pesoCotaSuperior', 'pesoCotaInferior', 'esCombo', 'detalleComboImp', 'detalleComboInf',
            'exentoIva', 'inafecto', 'exonerado', 'ivaDiferencial', 'tasaIva', 'tasaInternos', 'internosBulto',
            'tasaIibb', 'esAlcoholico', 'visibleMobile', 'esComodatable', 'desCortaArticulo', 'idPresentacionBulto',
            'desPresentacionBulto', 'idPresentacionUnidad', 'desPresentacionUnidad', 'idUnidadMedida',
            'desUnidadMedida', 'valorUnidadMedida', 'idArticuloEstadistico', 'codBarraBulto', 'codBarraUnidad',
            'tieneRetornables', 'bultosPallet', 'pisosPallet', 'apilabilidad', 'pesoBulto', 'llevaFrescura',
            'diasBloqueo', 'politicaStock', 'diasVentana', 'esActivoFijo', 'cantidadPuertas', 'unidadesFrente',
            'litrosRepago', 'idArtUsado', 'aniosAmortizacion'
        ]
        escritor_csv.writerow(encabezados)

        # Escribir los datos de los artículos
        for articulo in articulos:
            fila = [
                articulo.get('idArticulo', ''),
                articulo.get('desArticulo', ''),
                articulo.get('unidadesBulto', ''),
                articulo.get('anulado', ''),
                articulo.get('fechaAlta', ''),
                articulo.get('factorVenta', ''),
                articulo.get('minimoVenta', ''),
                articulo.get('pesable', ''),
                articulo.get('pesoCotaSuperior', ''),
                articulo.get('pesoCotaInferior', ''),
                articulo.get('esCombo', ''),
                articulo.get('detalleComboImp', ''),
                articulo.get('detalleComboInf', ''),
                articulo.get('exentoIva', ''),
                articulo.get('inafecto', ''),
                articulo.get('exonerado', ''),
                articulo.get('ivaDiferencial', ''),
                articulo.get('tasaIva', ''),
                articulo.get('tasaInternos', ''),
                articulo.get('internosBulto', ''),
                articulo.get('tasaIibb', ''),
                articulo.get('esAlcoholico', ''),
                articulo.get('visibleMobile', ''),
                articulo.get('esComodatable', ''),
                articulo.get('desCortaArticulo', ''),
                articulo.get('idPresentacionBulto', ''),
                articulo.get('desPresentacionBulto', ''),
                articulo.get('idPresentacionUnidad', ''),
                articulo.get('desPresentacionUnidad', ''),
                articulo.get('idUnidadMedida', ''),
                articulo.get('desUnidadMedida', ''),
                articulo.get('valorUnidadMedida', ''),
                articulo.get('idArticuloEstadistico', ''),
                articulo.get('codBarraBulto', ''),
                articulo.get('codBarraUnidad', ''),
                articulo.get('tieneRetornables', ''),
                articulo.get('bultosPallet', ''),
                articulo.get('pisosPallet', ''),
                articulo.get('apilabilidad', ''),
                articulo.get('pesoBulto', ''),
                articulo.get('llevaFrescura', ''),
                articulo.get('diasBloqueo', ''),
                articulo.get('politicaStock', ''),
                articulo.get('diasVentana', ''),
                articulo.get('esActivoFijo', ''),
                articulo.get('cantidadPuertas', ''),
                articulo.get('unidadesFrente', ''),
                articulo.get('litrosRepago', ''),
                articulo.get('idArtUsado', ''),
                articulo.get('aniosAmortizacion', '')
            ]
            escritor_csv.writerow(fila)

    print(f'El archivo CSV ha sido creado exitosamente en: {archivo_csv}')

# Especificar los nombres de los archivos de entrada y salida
archivo_json = 'ruta_al_archivo.json'
archivo_csv = 'ruta_de_salida.csv'

# Ejecutar la función
json_a_csv(archivo_json, archivo_csv)
