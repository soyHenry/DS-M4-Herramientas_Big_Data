CREATE DATABASE integrador;
USE integrador;
DROP TABLE IF EXISTS compra;
CREATE EXTERNAL TABLE IF NOT EXISTS compra (
  IdCompra				INTEGER,
  Fecha 				DATE,
  IdProducto			INTEGER,
  Cantidad			    INTEGER,
  Precio				FLOAT,
  IdProveedor			INTEGER
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    'separatorChar'=',',
    'skip.header.line.count'='1'
)
LOCATION '/user/instructor/integrador/compra';

SELECT * FROM compra limit 10;
SELECT COUNT(*) FROM compra;

DROP TABLE IF EXISTS gasto;
CREATE EXTERNAL TABLE IF NOT EXISTS gasto (
  IdGasto				INTEGER,
  IdSucursal			INTEGER,
  IdTipoGasto			INTEGER,
  Fecha 				DATE,
  Monto				    FLOAT
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    'separatorChar'=',',
    'skip.header.line.count'='1'
)
LOCATION '/user/instructor/integrador/gasto';

SELECT * FROM gasto limit 10;
SELECT COUNT(*) FROM gasto;

DROP TABLE IF EXISTS tipo_gasto;
CREATE EXTERNAL TABLE IF NOT EXISTS tipo_gasto (
  IdTipoGasto			INTEGER,
  Descripcion			VARCHAR(50),
  Monto_Aproximado	    FLOAT
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    'separatorChar'=',',
    'skip.header.line.count'='1'
)
LOCATION '/user/instructor/integrador/tipodegasto';

SELECT * FROM tipo_gasto limit 10;
SELECT COUNT(*) FROM tipo_gasto;

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
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    'separatorChar'=',',
    'skip.header.line.count'='1'
)
LOCATION '/user/instructor/integrador/venta';

SELECT * FROM venta limit 10;
SELECT COUNT(*) FROM venta;

DROP TABLE IF EXISTS canal_venta;
CREATE EXTERNAL TABLE IF NOT EXISTS canal_venta (
  IdCanal				INTEGER,
  Canal 				VARCHAR(50)
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    'separatorChar'=',',
    'skip.header.line.count'='1'
)
LOCATION '/user/instructor/integrador/canaldeventa';

SELECT * FROM canal_venta;
SELECT COUNT(*) FROM canal_venta;

DROP TABLE IF EXISTS cliente;
CREATE EXTERNAL TABLE IF NOT EXISTS cliente (
	ID					INTEGER,
	Provincia			VARCHAR(50),
	Nombre_y_Apellido	VARCHAR(80),
	Domicilio			VARCHAR(150),
	Telefono			VARCHAR(30),
	Edad				VARCHAR(5),
	Localidad			VARCHAR(80),
	X					VARCHAR(30),
	Y					VARCHAR(30),
	col10				VARCHAR(1)
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    'separatorChar'=';',
    'skip.header.line.count'='1'
)
LOCATION '/user/instructor/integrador/cliente';

SELECT * FROM cliente;
SELECT COUNT(*) FROM cliente;

DROP TABLE IF EXISTS producto;
CREATE EXTERNAL TABLE IF NOT EXISTS producto (
	IdProducto					INTEGER,
	Concepto					VARCHAR(100),
	Tipo						VARCHAR(50),
	Precio						FLOAT
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    'separatorChar'=';',
    'skip.header.line.count'='1'
)
LOCATION '/user/instructor/integrador/producto';

SELECT * FROM producto;
SELECT COUNT(*) FROM producto;

DROP TABLE IF EXISTS empleado;
CREATE EXTERNAL TABLE IF NOT EXISTS empleado (
	ID_empleado		INTEGER,
	Apellido		VARCHAR(50),
	Nombre	        VARCHAR(80),
	Sucursal		VARCHAR(150),
	Sector			VARCHAR(30),
	Cargo			VARCHAR(30),
	Salario			FLOAT
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    'separatorChar'=';',
    'skip.header.line.count'='1'
)
LOCATION '/user/instructor/integrador/empleado';

SELECT * FROM empleado;
SELECT COUNT(*) FROM empleado;

DROP TABLE IF EXISTS sucursal;
CREATE EXTERNAL TABLE IF NOT EXISTS sucursal (
	ID			INTEGER,
	Sucursal	VARCHAR(40),
	Domicilio	VARCHAR(150),
	Localidad	VARCHAR(80),
	Provincia	VARCHAR(50),
	Latitud2	VARCHAR(30),
	Longitud2	VARCHAR(30)
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    'separatorChar'=';',
    'skip.header.line.count'='1'
)
LOCATION '/user/instructor/integrador/sucursal';

SELECT * FROM sucursal;
SELECT COUNT(*) FROM sucursal;

DROP TABLE IF EXISTS calendario;
CREATE EXTERNAL TABLE calendario (
        id                      INTEGER,
        fecha                 	DATE,
        anio                    INTEGER,
        mes                   	INTEGER,
        dia                     INTEGER,
        trimestre               INTEGER, -- 1 to 4
        semana                  INTEGER, -- 1 to 52/53
        dia_nombre              VARCHAR(9), -- 'Monday', 'Tuesday'...
        mes_nombre              VARCHAR(9) -- 'January', 'February'...
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    'separatorChar'=',',
    'skip.header.line.count'='1'
)
LOCATION '/user/instructor/integrador/calendario';

SELECT * FROM calendario;
SELECT COUNT(*) FROM calendario;

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
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    'separatorChar'=',',
    'skip.header.line.count'='1'
)
LOCATION '/user/instructor/integrador/proveedor';

SELECT * FROM proveedor;
SELECT COUNT(*) FROM proveedor;