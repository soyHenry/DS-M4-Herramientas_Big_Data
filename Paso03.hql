CREATE DATABASE IF NOT EXISTS integrador2;
USE integrador2;

DROP TABLE IF EXISTS compra;
CREATE EXTERNAL TABLE IF NOT EXISTS compra (
  IdCompra				INTEGER,
  Fecha 				DATE,
  IdProducto			INTEGER,
  Cantidad			    INTEGER,
  Precio				FLOAT,
  IdProveedor			INTEGER
)
STORED AS PARQUET
LOCATION '/data2/compra'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

INSERT INTO compra
SELECT
	IdCompra,
	Fecha,
	IdProducto,
	Cantidad,
	Precio,
	IdProveedor
FROM 
    integrador.compra;

DROP TABLE IF EXISTS gasto;
CREATE EXTERNAL TABLE IF NOT EXISTS gasto (
  IdGasto				INTEGER,
  IdSucursal			INTEGER,
  Fecha 				DATE,
  Monto				    FLOAT
)
PARTITIONED BY(IdTipoGasto INTEGER)
STORED AS PARQUET
LOCATION '/data2/gasto'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

INSERT INTO gasto
PARTITION(IdTipoGasto=1)
SELECT
	IdGasto,
	IdSucursal,
	Fecha,
	Monto
FROM integrador.gasto
WHERE IdTipoGasto=1;

INSERT INTO gasto
PARTITION(IdTipoGasto=2)
SELECT
	IdGasto,
	IdSucursal,
	Fecha,
	Monto
FROM integrador.gasto
WHERE IdTipoGasto=2;

INSERT INTO gasto
PARTITION(IdTipoGasto=3)
SELECT
	IdGasto,
	IdSucursal,
	Fecha,
	Monto
FROM integrador.gasto
WHERE IdTipoGasto=3;

INSERT INTO gasto
PARTITION(IdTipoGasto=4)
SELECT
	IdGasto,
	IdSucursal,
	Fecha,
	Monto
FROM integrador.gasto
WHERE IdTipoGasto=4;

DROP TABLE IF EXISTS tipo_gasto;
CREATE EXTERNAL TABLE IF NOT EXISTS tipo_gasto (
  IdTipoGasto			INTEGER,
  Descripcion			VARCHAR(50),
  Monto_Aproximado	    FLOAT
)
STORED AS PARQUET
LOCATION '/data2/tipodegasto'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

INSERT INTO tipo_gasto
SELECT
	IdTipoGasto,
	Descripcion,
	Monto_Aproximado
FROM integrador.tipo_gasto;

DROP TABLE IF EXISTS venta;
CREATE EXTERNAL TABLE IF NOT EXISTS venta (
  IdVenta				INTEGER,
  Fecha 				DATE,
  Fecha_Entrega 		DATE,
  IdCanal				INTEGER, 
  IdCliente			INTEGER, 
  IdSucursal			INTEGER,
  IdEmpleado			INTEGER,
  IdProducto			INTEGER,
  Precio				FLOAT,
  Cantidad			INTEGER
)
STORED AS PARQUET
LOCATION '/data2/venta'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

INSERT INTO venta
SELECT 	
	IdVenta,
	Fecha,
	Fecha_Entrega,
	IdCanal,
	IdCliente,
	IdSucursal,
	IdEmpleado,
	IdProducto,
	Precio,
	Cantidad
FROM integrador.venta;

DROP TABLE IF EXISTS canal_venta;
CREATE EXTERNAL TABLE IF NOT EXISTS canal_venta (
  IdCanal				INTEGER,
  Canal 				VARCHAR(50)
)
STORED AS PARQUET
LOCATION '/data2/canaldeventa'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

INSERT INTO canal_venta
SELECT
	IdCanal,
	Canal
FROM integrador.canal_venta;

DROP TABLE IF EXISTS cliente;
CREATE EXTERNAL TABLE IF NOT EXISTS cliente (
	IdCliente			INTEGER,
	Provincia			VARCHAR(50),
	Nombre_y_Apellido	VARCHAR(80),
	Domicilio			VARCHAR(150),
	Telefono			VARCHAR(30),
	Edad				VARCHAR(5),
	Localidad			VARCHAR(80),
	Longitud			FLOAT,
	Latitud				FLOAT
)
STORED AS PARQUET
LOCATION '/data2/cliente'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

INSERT INTO cliente
(   idcliente,
	provincia,
	nombre_y_apellido,
	domicilio,
	telefono,
	edad,
	localidad,
	longitud,
	latitud)
SELECT
	ID,
	Provincia,
	Nombre_y_Apellido,
	Domicilio,
	Telefono,
	Edad,
	Localidad,
	REPLACE(Y, ',', '.'),
	REPLACE(X, ',', '.')
FROM integrador.cliente;

DROP TABLE IF EXISTS producto;
CREATE EXTERNAL TABLE IF NOT EXISTS producto (
	IdProducto					INTEGER,
	Descripcion					VARCHAR(100),
	Tipo						VARCHAR(50),
	Precio						FLOAT
)
STORED AS PARQUET
LOCATION '/data2/producto'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

INSERT INTO producto
	(idproducto,
	descripcion,
	tipo,
	precio)
SELECT
	IdProducto,
	Concepto,
	Tipo,
	REPLACE(Precio, ',', '.')
FROM integrador.producto;

DROP TABLE IF EXISTS empleado;
CREATE EXTERNAL TABLE IF NOT EXISTS empleado (
	CodigoEmpleado	INTEGER,
	Apellido		VARCHAR(50),
	Nombre	        VARCHAR(80),
	Sucursal		VARCHAR(150),
	Sector			VARCHAR(30),
	Cargo			VARCHAR(30),
	Salario			FLOAT
)
STORED AS PARQUET
LOCATION '/data2/empleado'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

INSERT INTO empleado
	(codigoempleado,
	apellido,
	nombre,
	sucursal,
	sector,
	cargo,
	salario)
SELECT
	ID_empleado,
	Apellido,
	Nombre,
	Sucursal,
	Sector,
	Cargo,
	REPLACE(Salario. ',', '.')
FROM integrador.empleado;

DROP TABLE IF EXISTS sucursal;
CREATE EXTERNAL TABLE IF NOT EXISTS sucursal (
	IdSucursal	INTEGER,
	Sucursal	VARCHAR(40),
	Domicilio	VARCHAR(150),
	Localidad	VARCHAR(80),
	Provincia	VARCHAR(50),
	Latitud		FLOAT,
	Longitud	FLOAT
)
STORED AS PARQUET
LOCATION '/data2/sucursal'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

INSERT INTO sucursal
SELECT	
	IdSucursal,
	Sucursal,
	Domicilio,
	Localidad,
	Provincia,
	REPLACE(Latitud, ',', '.'),
	REPLACE(Longitud, ',', '.')
FROM integrador.sucursal;

DROP TABLE IF EXISTS calendario;
CREATE EXTERNAL TABLE calendario (
        id                      INTEGER,
        fecha                 	DATE,
        anio                    INTEGER,
        mes                   	INTEGER,
        dia                     INTEGER,
        trimestre               INTEGER,
        semana                  INTEGER,
        dia_nombre              VARCHAR(9),
        mes_nombre              VARCHAR(9)
)
STORED AS PARQUET
LOCATION '/data2/calendario'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

INSERT INTO calendario
SELECT
	id,
	fecha,
	anio,
	mes,
	trimestre,
	semana,
	dia_nombre,
	mes_nombre
FROM integrador.calendario;

DROP TABLE IF EXISTS proveedor;
CREATE EXTERNAL TABLE IF NOT EXISTS proveedor (
	IDProveedor			INTEGER,
	Nombre	VARCHAR(40),
	Address	VARCHAR(150),
	City	VARCHAR(80),
	State	VARCHAR(50),
	Country	VARCHAR(20),
	departamen	VARCHAR(50)
)
STORED AS PARQUET
LOCATION '/data2/proveedor'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

INSERT INTO proveedor
SELECT
	IdProveedor	,
	Nombre,
	Direccion,
	Ciudad,
	Provincia,
	Pais,
	Departamento
FROM integrador.proveedor;
	







--particiones
DROP TABLE trips_part;
CREATE EXTERNAL TABLE trips_part(
	bikeid INT,
	checkout_time STRING,
	duration_minutes INT,
	end_station_id INT,
	end_station_name STRING,
	start_station_id INT,
	start_station_name STRING,
	start_time TIMESTAMP,
	subscriber_type STRING,
	trip_id BIGINT,
	year INT
)
PARTITIONED BY(month INT)
LOCATION '/user/instructor/data/bikeshare/trips_part/';

-- Ejecutar de 1 a 12
INSERT INTO trips_part
PARTITION(month=12)
SELECT bikeid,
	checkout_time,
	duration_minutes,
	end_station_id,
	end_station_name,
	start_station_id,
	start_station_name,
	start_time,
	subscriber_type,
	trip_id,
	year_modif
FROM trips_ok
WHERE month_modif = 12;