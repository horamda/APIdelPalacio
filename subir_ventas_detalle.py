import json
import mysql.connector
import time

# Conexión a la base de datos MySQL
db_config = {
    'host': '190.210.132.63',
    'user': 'ht627842_admin',
    'password': 'Paisaje.2024*',
    'database': 'ht627842_pepe'
}

# Abrir el archivo JSON
with open('c:/Users/horac/Desktop/Proyectos WEB/APIdelpalacio/resultados_ventas.json', 'r', encoding='utf-8') as file:
    data = json.load(file)

# Verificar que el archivo JSON sea una lista
if isinstance(data, list):
    ventas_resumen = data  # Asignar la lista a ventas_resumen
else:
    raise ValueError("El archivo JSON no contiene una lista de ventas.")

# Definir la inserción por lotes y el límite del tamaño del lote
batch_size = 100
total_registros = len(ventas_resumen)

# Conectar a la base de datos
connection = mysql.connector.connect(**db_config)
cursor = connection.cursor()

# Crear la tabla detalle_ventas (aquí se crea si no existe)
create_table_query = """
CREATE TABLE IF NOT EXISTS detalle_ventas (
    idEmpresa INT,
    dsEmpresa VARCHAR(255),
    idDocumento VARCHAR(255),
    dsDocumento VARCHAR(255),
    letra VARCHAR(1),
    serie INT,
    nrodoc INT,
    anulado VARCHAR(10),
    idMovComercial INT,
    dsMovComercial VARCHAR(255),
    idRechazo INT,
    dsRechazo VARCHAR(255),
    fechaComprobate DATE,
    fechaAlta DATE,
    usuarioAlta VARCHAR(255),
    fechaVencimiento DATE,
    fechaEntrega DATE,
    idSucursal INT,
    dsSucursal VARCHAR(255),
    idFuerzaVentas INT,
    dsFuerzaVentas VARCHAR(255),
    idDeposito INT,
    dsDeposito VARCHAR(255),
    idVendedor INT,
    dsVendedor VARCHAR(255),
    idSupervisor INT,
    dsSupervisor VARCHAR(255),
    idGerente INT,
    dsGerente VARCHAR(255),
    tipoConstribuyente VARCHAR(255),
    dsTipoConstribuyente VARCHAR(255),
    idTipoPago INT,
    dsTipoPago VARCHAR(255),
    fechaPago DATE,
    idPedido INT,
    fechaPedido DATE,
    origen VARCHAR(255),
    planillaCarga VARCHAR(255),
    idFleteroCarga INT,
    dsFleteroCarga VARCHAR(255),
    idLiquidacion INT,
    fechaLiquidacion DATE,
    idCaja INT,
    fechaCaja DATE,
    cajero VARCHAR(255),
    idCliente INT,
    nombreCliente VARCHAR(255),
    domicilioCliente VARCHAR(255),
    codigoPostal INT,
    dsLocalidad VARCHAR(255),
    idProvincia VARCHAR(10),
    dsProvincia VARCHAR(255),
    idNegocio INT,
    dsNegocio VARCHAR(255),
    idAgrupacion INT,
    dsAgrupacion VARCHAR(255),
    idArea INT,
    dsArea VARCHAR(255),
    idSegmentoMkt INT,
    dsSegmentoMkt VARCHAR(255),
    idCanalMkt INT,
    dsCanalMkt VARCHAR(255),
    idSubcanalMkt INT,
    dsSegmentoMkt2 VARCHAR(255),
    idLinea INT,
    idArticulo INT,
    dsArticulo VARCHAR(255),
    idConcepto INT,
    dsConcepto VARCHAR(255),
    esCombo VARCHAR(10),
    idCombo INT,
    idArticuloEstadistico INT,
    dsArticuloEstadistico VARCHAR(255),
    presentacionArticulo INT,
    cantidadPorPallets DECIMAL(10, 2),
    peso DECIMAL(10, 2),
    cantidadSolicitada DECIMAL(10, 2),
    unidadesSolicitadas DECIMAL(10, 2),
    cantidadesCorCargo DECIMAL(10, 2),
    cantidadesSinCargo DECIMAL(10, 2),
    cantidadesTotal DECIMAL(10, 2),
    pesoTotal DECIMAL(10, 2),
    cantidadesRechazo DECIMAL(10, 2),
    unimedcargo DECIMAL(10, 2),
    unimedscargo DECIMAL(10, 2),
    unimedtotal DECIMAL(10, 2),
    precioUnitarioBruto DECIMAL(10, 2),
    bonificacion DECIMAL(10, 2),
    precioUnitarioNeto DECIMAL(10, 2),
    subtotalBruto DECIMAL(10, 2),
    subtotalBonificado DECIMAL(10, 2),
    subtotalNeto DECIMAL(10, 2),
    iva21 DECIMAL(10, 2),
    iva27 DECIMAL(10, 2),
    iva105 DECIMAL(10, 2),
    internos DECIMAL(10, 2),
    subtotalFinal DECIMAL(10, 2)
);
"""
cursor.execute(create_table_query)

# Insertar datos por lotes
insert_query = """
INSERT INTO detalle_ventas 
(idEmpresa, dsEmpresa, idDocumento, dsDocumento, letra, serie, nrodoc, anulado, idMovComercial, dsMovComercial, idRechazo, 
dsRechazo, fechaComprobate, fechaAlta, usuarioAlta, fechaVencimiento, fechaEntrega, idSucursal, dsSucursal, idFuerzaVentas, 
dsFuerzaVentas, idDeposito, dsDeposito, idVendedor, dsVendedor, idSupervisor, dsSupervisor, idGerente, dsGerente, 
tipoConstribuyente, dsTipoConstribuyente, idTipoPago, dsTipoPago, fechaPago, idPedido, fechaPedido, origen, planillaCarga, 
idFleteroCarga, dsFleteroCarga, idLiquidacion, fechaLiquidacion, idCaja, fechaCaja, cajero, idCliente, nombreCliente, 
domicilioCliente, codigoPostal, dsLocalidad, idProvincia, dsProvincia, idNegocio, dsNegocio, idAgrupacion, dsAgrupacion, 
idArea, dsArea, idSegmentoMkt, dsSegmentoMkt, idCanalMkt, dsCanalMkt, idSubcanalMkt, dsSegmentoMkt2, idLinea, idArticulo, 
dsArticulo, idConcepto, dsConcepto, esCombo, idCombo, idArticuloEstadistico, dsArticuloEstadistico, presentacionArticulo, 
cantidadPorPallets, peso, cantidadSolicitada, unidadesSolicitadas, cantidadesCorCargo, cantidadesSinCargo, cantidadesTotal, 
pesoTotal, cantidadesRechazo, unimedcargo, unimedscargo, unimedtotal, precioUnitarioBruto, bonificacion, precioUnitarioNeto, 
subtotalBruto, subtotalBonificado, subtotalNeto, iva21, iva27, iva105, internos, subtotalFinal)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""

# Registrar el tiempo de inicio
start_time = time.time()

# Procesar los datos en lotes
for i in range(0, total_registros, batch_size):
    batch = ventas_resumen[i:i + batch_size]
    values = [(item.get('idEmpresa', None), item.get('dsEmpresa', None), item.get('idDocumento', None), item.get('dsDocumento', None),
               item.get('letra', None), item.get('serie', None), item.get('nrodoc', None), item.get('anulado', None),
               item.get('idMovComercial', None), item.get('dsMovComercial', None), item.get('idRechazo', None), item.get('dsRechazo', None),
               item.get('fechaComprobate', None), item.get('fechaAlta', None), item.get('usuarioAlta', None), item.get('fechaVencimiento', None),
               item.get('fechaEntrega', None), item.get('idSucursal', None), item.get('dsSucursal', None), item.get('idFuerzaVentas', None),
               item.get('dsFuerzaVentas', None), item.get('idDeposito', None), item.get('dsDeposito', None), item.get('idVendedor', None),
               item.get('dsVendedor', None), item.get('idSupervisor', None), item.get('dsSupervisor', None), item.get('idGerente', None),
               item.get('dsGerente', None), item.get('tipoConstribuyente', None), item.get('dsTipoConstribuyente', None),
               item.get('idTipoPago', None), item.get('dsTipoPago', None), item.get('fechaPago', None), item.get('idPedido', None),
               item.get('fechaPedido', None), item.get('origen', None), item.get('planillaCarga', None), item.get('idFleteroCarga', None),
               item.get('dsFleteroCarga', None), item.get('idLiquidacion', None), item.get('fechaLiquidacion', None), item.get('idCaja', None),
               item.get('fechaCaja', None), item.get('cajero', None), item.get('idCliente', None), item.get('nombreCliente', None),
               item.get('domicilioCliente', None), item.get('codigoPostal', None), item.get('dsLocalidad', None), item.get('idProvincia', None),
               item.get('dsProvincia', None), item.get('idNegocio', None), item.get('dsNegocio', None), item.get('idAgrupacion', None),
               item.get('dsAgrupacion', None), item.get('idArea', None), item.get('dsArea', None), item.get('idSegmentoMkt', None),
               item.get('dsSegmentoMkt', None), item.get('idCanalMkt', None), item.get('dsCanalMkt', None), item.get('idSubcanalMkt', None),
               item.get('dsSegmentoMkt2', None), item.get('idLinea', None), item.get('idArticulo', None), item.get('dsArticulo', None),
               item.get('idConcepto', None), item.get('dsConcepto', None), item.get('esCombo', None), item.get('idCombo', None),
               item.get('idArticuloEstadistico', None), item.get('dsArticuloEstadistico', None), item.get('presentacionArticulo', None),
               item.get('cantidadPorPallets', None), item.get('peso', None), item.get('cantidadSolicitada', None),
               item.get('unidadesSolicitadas', None), item.get('cantidadesCorCargo', None), item.get('cantidadesSinCargo', None),
               item.get('cantidadesTotal', None), item.get('pesoTotal', None), item.get('cantidadesRechazo', None), item.get('unimedcargo', None),
               item.get('unimedscargo', None), item.get('unimedtotal', None), item.get('precioUnitarioBruto', None), item.get('bonificacion', None),
               item.get('precioUnitarioNeto', None), item.get('subtotalBruto', None), item.get('subtotalBonificado', None),
               item.get('subtotalNeto', None), item.get('iva21', None), item.get('iva27', None), item.get('iva105', None), item.get('internos', None),
               item.get('subtotalFinal', None)) for item in batch]

    cursor.executemany(insert_query, values)
    connection.commit()

# Registrar el tiempo final
end_time = time.time()

# Mostrar el tiempo transcurrido
elapsed_time = end_time - start_time
print(f"Tiempo total de inserción: {elapsed_time:.2f} segundos")

# Cerrar la conexión
cursor.close()
connection.close()
