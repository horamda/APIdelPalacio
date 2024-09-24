import mysql.connector
from mysql.connector import Error
import json
import time

# Configuración de conexión MySQL
#conexion= mysql.connector.connect(
#        host="bexxqnxj8z509ivyufod-mysql.services.clever-cloud.com",
#        user="upxj8xta7uyngg8e",
#        password="mbv7NFyuwUMNc5oneQ8u",
#        database="bexxqnxj8z509ivyufod",
#        port=3306
#    conexion)
conexion = mysql.connector.connect(
        host='190.210.132.63',
        database='ht627842_apichess',
        user='ht627842_admin',
        password='Paisaje.2024*'
    )


cursor = conexion.cursor()

# Crear tablas (si no existen)
cursor.execute("""
CREATE TABLE IF NOT EXISTS articulos (
    idArticulo INT PRIMARY KEY,
    desArticulo VARCHAR(255),
    unidadesBulto INT,
    anulado BOOLEAN,
    fechaAlta DATE,
    factorVenta DECIMAL(10, 2),
    minimoVenta DECIMAL(10, 2),
    pesable BOOLEAN,
    pesoCotaSuperior DECIMAL(10, 2),
    pesoCotaInferior DECIMAL(10, 2),
    esCombo BOOLEAN,
    detalleComboImp BOOLEAN,
    detalleComboInf BOOLEAN,
    exentoIva BOOLEAN,
    inafecto BOOLEAN,
    exonerado BOOLEAN,
    ivaDiferencial BOOLEAN,
    tasaIva DECIMAL(10, 2),
    tasaInternos DECIMAL(10, 2),
    internosBulto DECIMAL(10, 2),
    tasaIibb DECIMAL(10, 2),
    esAlcoholico BOOLEAN,
    visibleMobile BOOLEAN,
    esComodatable BOOLEAN,
    desCortaArticulo VARCHAR(255),
    idPresentacionBulto VARCHAR(50),
    desPresentacionBulto VARCHAR(255),
    idPresentacionUnidad VARCHAR(50),
    desPresentacionUnidad VARCHAR(255),
    idUnidadMedida INT,
    desUnidadMedida VARCHAR(255),
    valorUnidadMedida DECIMAL(10, 2),
    idArticuloEstadistico INT,
    codBarraBulto VARCHAR(255),
    codBarraUnidad VARCHAR(255),
    tieneRetornables BOOLEAN,
    bultosPallet INT,
    pisosPallet INT,
    apilabilidad DECIMAL(10, 2),
    pesoBulto DECIMAL(10, 2),
    llevaFrescura BOOLEAN,
    diasBloqueo INT,
    politicaStock INT,
    diasVentana INT,
    esActivoFijo BOOLEAN,
    cantidadPuertas INT,
    unidadesFrente INT,
    litrosRepago DECIMAL(10, 2),
    idArtUsado INT,
    aniosAmortizacion INT
);
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS agrupaciones (
    id INT AUTO_INCREMENT PRIMARY KEY,
    idArticulo INT,
    idFormaAgrupar VARCHAR(255),
    desFormaAgrupar VARCHAR(255),
    idAgrupacion VARCHAR(255),
    desAgrupacion VARCHAR(255),
    FOREIGN KEY (idArticulo) REFERENCES articulos(idArticulo)
);
""")

# Medir el tiempo de ejecución
start_time = time.time()

# Leer el archivo JSON
with open('resultados_articulos.json', 'r', encoding='utf-8') as file:
    data = json.load(file)

# Borrar datos existentes
cursor.execute("DELETE FROM agrupaciones")
cursor.execute("DELETE FROM articulos")

# Insertar datos en la tabla 'articulos' y 'agrupaciones'
articulos_data = []
agrupaciones_data = []

for item in data.get('Articulos', {}).get('eArticulos', []):
    # Preparar datos para 'articulos'
    articulos_data.append((
        item.get('idArticulo'),
        item.get('desArticulo'),
        item.get('unidadesBulto'),
        item.get('anulado'),
        item.get('fechaAlta'),
        item.get('factorVenta'),
        item.get('minimoVenta'),
        item.get('pesable'),
        item.get('pesoCotaSuperior'),
        item.get('pesoCotaInferior'),
        item.get('esCombo'),
        item.get('detalleComboImp'),
        item.get('detalleComboInf'),
        item.get('exentoIva'),
        item.get('inafecto'),
        item.get('exonerado'),
        item.get('ivaDiferencial'),
        item.get('tasaIva'),
        item.get('tasaInternos'),
        item.get('internosBulto'),
        item.get('tasaIibb'),
        item.get('esAlcoholico'),
        item.get('visibleMobile'),
        item.get('esComodatable'),
        item.get('desCortaArticulo'),
        item.get('idPresentacionBulto'),
        item.get('desPresentacionBulto'),
        item.get('idPresentacionUnidad'),
        item.get('desPresentacionUnidad'),
        item.get('idUnidadMedida'),
        item.get('desUnidadMedida'),
        item.get('valorUnidadMedida'),
        item.get('idArticuloEstadistico'),
        item.get('codBarraBulto'),
        item.get('codBarraUnidad'),
        item.get('tieneRetornables'),
        item.get('bultosPallet'),
        item.get('pisosPallet'),
        item.get('apilabilidad'),
        item.get('pesoBulto'),
        item.get('llevaFrescura'),
        item.get('diasBloqueo'),
        item.get('politicaStock'),
        item.get('diasVentana'),
        item.get('esActivoFijo'),
        item.get('cantidadPuertas'),
        item.get('unidadesFrente'),
        item.get('litrosRepago'),
        item.get('idArtUsado'),
        item.get('aniosAmortizacion')
    ))
    
    # Preparar datos para 'agrupaciones'
    for agrupacion in item.get('eAgrupaciones', []):
        agrupaciones_data.append((
            item.get('idArticulo'),
            agrupacion.get('idFormaAgrupar'),
            agrupacion.get('desFormaAgrupar'),
            agrupacion.get('idAgrupacion'),
            agrupacion.get('desAgrupacion')
        ))

# Inserción por lotes
cursor.executemany("""
    INSERT INTO articulos (
        idArticulo, desArticulo, unidadesBulto, anulado, fechaAlta, factorVenta, minimoVenta, pesable,
        pesoCotaSuperior, pesoCotaInferior, esCombo, detalleComboImp, detalleComboInf, exentoIva, inafecto,
        exonerado, ivaDiferencial, tasaIva, tasaInternos, internosBulto, tasaIibb, esAlcoholico, visibleMobile,
        esComodatable, desCortaArticulo, idPresentacionBulto, desPresentacionBulto, idPresentacionUnidad,
        desPresentacionUnidad, idUnidadMedida, desUnidadMedida, valorUnidadMedida, idArticuloEstadistico,
        codBarraBulto, codBarraUnidad, tieneRetornables, bultosPallet, pisosPallet, apilabilidad, pesoBulto,
        llevaFrescura, diasBloqueo, politicaStock, diasVentana, esActivoFijo, cantidadPuertas, unidadesFrente,
        litrosRepago, idArtUsado, aniosAmortizacion
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
              %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
""", articulos_data)

cursor.executemany("""
    INSERT INTO agrupaciones (
        idArticulo, idFormaAgrupar, desFormaAgrupar, idAgrupacion, desAgrupacion
    ) VALUES (%s, %s, %s, %s, %s)
""", agrupaciones_data)

# Confirmar los cambios
conexion.commit()

# Cerrar la conexión
cursor.close()
conexion.close()

end_time = time.time()
execution_time = end_time - start_time

print(f"Datos cargados exitosamente en la base de datos en {execution_time} segundos.")
