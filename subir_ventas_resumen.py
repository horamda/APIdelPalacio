import mysql.connector
from mysql.connector import Error
import json
import time

def insert_data_in_batches(data, cursor, conn, batch_size=1000):
    batch_values = []
    total_inserted = 0
    
    start_time = time.time()  # Tiempo de inicio del lote

    # Definición de la consulta SQL fuera del bucle para que esté disponible en todo momento
    sql = """
    INSERT INTO ventas_comprobantes (
        idEmpresa, dsEmpresa, idDocumento, dsDocumento, letra, serie, nrodoc, anulado, idMovComercial, dsMovComercial,
        idRechazo, dsRechazo, fechaComprobate, fechaAlta, usuarioAlta, fechaVencimiento, fechaEntrega, idSucursal,
        dsSucursal, idFuerzaVentas, dsFuerzaVentas, idDeposito, dsDeposito, idVendedor, dsVendedor, idSupervisor,
        dsSupervisor, idGerente, dsGerente, tipoConstribuyente, dsTipoConstribuyente, idTipoPago, dsTipoPago, 
        fechaPago, idPedido, fechaPedido, origen, planillaCarga, idFleteroCarga, dsFleteroCarga, idLiquidacion, 
        fechaLiquidacion, idCaja, fechaCaja, cajero, idCliente, nombreCliente, domicilioCliente, codigoPostal, 
        dsLocalidad, idProvincia, dsProvincia, idNegocio, dsNegocio, idAgrupacion, dsAgrupacion, idArea, dsArea, 
        idSegmentoMkt, dsSegmentoMkt, idCanalMkt, dsCanalMkt, idSubcanalMkt, dsSegmentoMktAlternativo, fechaAsientoContable, 
        nroAsientoContable, nroPlanContable, codCuentaContable, idCentroCosto, dsCuentaContable, subtotalBruto, 
        subtotalBonificado, subtotalNeto, iva21, iva27, iva105, per3337, iva2, percepcion212, percepcioniibb, internos, 
        subtotalFinal, tradespendg, tradespends, tradespendb, tradespendi, tradespendp, tradespendt, totradspend, 
        acciones, persiibbd, persiibbr, numerosserie, numerosactivo, cuentayorden, codprovcyo, descrip, nrorendcyo, 
        idTipoCambio, dsTipoCambio, cfdiEmitido, regimenFiscal, informado, firmadigital, proveedor, fvigpcompra, 
        preciocomprabr, preciocomprant, lineaCredito, numeracionFiscal, codproviibb
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
              %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
              %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
              %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
              %s, %s, %s, %s,%s,%s,%s,%s,%s,%s,%s)
    """

    for venta in data['dsReporteComprobantesApi']['VentasResumen']:
        # Manejo condicional del campo dsSegmentoMktAlternativo
        dsSegmentoMktAlternativo = venta.get('dsSegmentoMktAlternativo', venta.get('dsSegmentoMkt', None))

        batch_values.append((
            venta['idEmpresa'], venta['dsEmpresa'], venta['idDocumento'], venta['dsDocumento'],
            venta['letra'], venta['serie'], venta['nrodoc'], venta['anulado'], venta['idMovComercial'],
            venta['dsMovComercial'], venta['idRechazo'], venta['dsRechazo'], venta['fechaComprobate'],
            venta['fechaAlta'], venta['usuarioAlta'], venta['fechaVencimiento'], venta['fechaEntrega'],
            venta['idSucursal'], venta['dsSucursal'], venta['idFuerzaVentas'], venta['dsFuerzaVentas'],
            venta['idDeposito'], venta['dsDeposito'], venta['idVendedor'], venta['dsVendedor'],
            venta['idSupervisor'], venta['dsSupervisor'], venta['idGerente'], venta['dsGerente'],
            venta['tipoConstribuyente'], venta['dsTipoConstribuyente'], venta['idTipoPago'], venta['dsTipoPago'],
            venta['fechaPago'], venta['idPedido'], venta['fechaPedido'], venta['origen'], venta['planillaCarga'],
            venta['idFleteroCarga'], venta['dsFleteroCarga'], venta['idLiquidacion'], venta['fechaLiquidacion'],
            venta['idCaja'], venta['fechaCaja'], venta['cajero'], venta['idCliente'], venta['nombreCliente'],
            venta['domicilioCliente'], venta['codigoPostal'], venta['dsLocalidad'], venta['idProvincia'],
            venta['dsProvincia'], venta['idNegocio'], venta['dsNegocio'], venta['idAgrupacion'], venta['dsAgrupacion'],
            venta['idArea'], venta['dsArea'], venta['idSegmentoMkt'], venta['dsSegmentoMkt'], venta['idCanalMkt'],
            venta['dsCanalMkt'], venta['idSubcanalMkt'], dsSegmentoMktAlternativo, venta['fechaAsientoContable'],
            venta['nroAsientoContable'], venta['nroPlanContable'], venta['codCuentaContable'], venta['idCentroCosto'],
            venta['dsCuentaContable'], venta['subtotalBruto'], venta['subtotalBonificado'], venta['subtotalNeto'],
            venta['iva21'], venta['iva27'], venta['iva105'], venta['per3337'], venta['iva2'], venta['percepcion212'],
            venta['percepcioniibb'], venta['internos'], venta['subtotalFinal'], venta['tradespendg'], venta['tradespends'],
            venta['tradespendb'], venta['tradespendi'], venta['tradespendp'], venta['tradespendt'], venta['totradspend'],
            venta['acciones'], venta['persiibbd'], venta['persiibbr'], venta['numerosserie'], venta['numerosactivo'],
            venta['cuentayorden'], venta['codprovcyo'], venta['descrip'], venta['nrorendcyo'], venta['idTipoCambio'],
            venta['dsTipoCambio'], venta['cfdiEmitido'], venta['regimenFiscal'], venta['informado'], venta['firmadigital'],
            venta['proveedor'], venta['fvigpcompra'], venta['preciocomprabr'], venta['preciocomprant'], venta['lineaCredito'],
            venta['numeracionFiscal'], venta['codproviibb']
        ))

        # Insertar por lotes
        if len(batch_values) == batch_size:
            cursor.executemany(sql, batch_values)
            conn.commit()
            total_inserted += len(batch_values)
            batch_values = []  # Reiniciar la lista
            end_time = time.time()
            print(f"Tiempo para insertar {batch_size} registros: {end_time - start_time:.2f} segundos")
            start_time = time.time()  # Reiniciar tiempo de inicio

    # Insertar cualquier valor restante que no completó un lote
    if batch_values:
        cursor.executemany(sql, batch_values)
        conn.commit()
        total_inserted += len(batch_values)
        end_time = time.time()
        print(f"Tiempo para insertar el último lote de {len(batch_values)} registros: {end_time - start_time:.2f} segundos")

    print(f"Total de registros insertados: {total_inserted}")

try:
    start_time = time.time()  # Tiempo de inicio total

   # # Conectar a la base de datos en Clever Cloud
   # conn = mysql.connector.connect(
   #     host="bexxqnxj8z509ivyufod-mysql.services.clever-cloud.com",
   #     user="upxj8xta7uyngg8e",
   #     password="mbv7NFyuwUMNc5oneQ8u",
   #     database="bexxqnxj8z509ivyufod",
   #     port=3306
   # )
   
    conn = mysql.connector.connect(
        host='190.210.132.63',
        database='ht627842_apichess',
        user='ht627842_admin',
        password='Paisaje.2024*'
    )

    if conn.is_connected():
        print("Conexión exitosa a la base de datos.")
        
        # Leer el archivo JSON con la codificación correcta
        with open('varios\ventassindetalle.json', 'r', encoding='utf-8') as file:
            data = json.load(file)
        
        cursor = conn.cursor()

        # Eliminar la tabla si existe
        cursor.execute("DROP TABLE IF EXISTS ventas_comprobantes")
        
        # Crear la tabla de nuevo
        create_table_query = """
        CREATE TABLE ventas_comprobantes (
            idEmpresa INT,
            dsEmpresa VARCHAR(255),
            idDocumento VARCHAR(10),
            dsDocumento VARCHAR(50),
            letra CHAR(1),
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
            tipoConstribuyente VARCHAR(10),
            dsTipoConstribuyente VARCHAR(255),
            idTipoPago INT,
            dsTipoPago VARCHAR(50),
            fechaPago DATE,
            idPedido INT,
            fechaPedido DATE,
            origen VARCHAR(50),
            planillaCarga VARCHAR(50),
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
            idProvincia CHAR(2),
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
            dsSegmentoMktAlternativo VARCHAR(255),
            fechaAsientoContable DATE,
            nroAsientoContable INT,
            nroPlanContable INT,
            codCuentaContable INT,
            idCentroCosto INT,
            dsCuentaContable VARCHAR(255),
            subtotalBruto DECIMAL(15, 2),
            subtotalBonificado DECIMAL(15, 2),
            subtotalNeto DECIMAL(15, 2),
            iva21 DECIMAL(15, 2),
            iva27 DECIMAL(15, 2),
            iva105 DECIMAL(15, 2),
            per3337 DECIMAL(15, 2),
            iva2 DECIMAL(15, 2),
            percepcion212 DECIMAL(15, 2),
            percepcioniibb DECIMAL(15, 2),
            internos DECIMAL(15, 2),
            subtotalFinal DECIMAL(15, 2),
            tradespendg DECIMAL(15, 2),
            tradespends DECIMAL(15, 2),
            tradespendb DECIMAL(15, 2),
            tradespendi DECIMAL(15, 2),
            tradespendp DECIMAL(15, 2),
            tradespendt DECIMAL(15, 2),
            totradspend DECIMAL(15, 2),
            acciones VARCHAR(255),
            persiibbd VARCHAR(255),
            persiibbr VARCHAR(255),
            numerosserie VARCHAR(255),
            numerosactivo VARCHAR(255),
            cuentayorden VARCHAR(255),
            codprovcyo INT,
            descrip VARCHAR(255),
            nrorendcyo INT,
            idTipoCambio INT,
            dsTipoCambio VARCHAR(255),
            cfdiEmitido VARCHAR(255),
            regimenFiscal VARCHAR(50),
            informado VARCHAR(10),
            firmadigital VARCHAR(255),
            proveedor VARCHAR(255),
            fvigpcompra DATE,
            preciocomprabr DECIMAL(15, 2),
            preciocomprant DECIMAL(15, 2),
            lineaCredito VARCHAR(255),
            numeracionFiscal VARCHAR(255),
            codproviibb VARCHAR(255)
        )
        """
        cursor.execute(create_table_query)
        print("Tabla ventas_comprobantes creada con éxito.")

        # Insertar datos en lotes
        insert_data_in_batches(data, cursor, conn, batch_size=1000)

except Error as e:
    print("Error al conectar o interactuar con la base de datos:", e)

finally:
    if conn.is_connected():
        cursor.close()
        conn.close()
        print("Conexión cerrada.")
        total_time = time.time() - start_time
        print(f"Tiempo total del proceso: {total_time:.2f} segundos")
