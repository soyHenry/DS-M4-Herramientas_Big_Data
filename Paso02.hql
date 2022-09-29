CREATE DATABASE IF NOT EXISTS integrador;
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
LOCATION '/data/compra';

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
LOCATION '/data/gasto';

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
LOCATION '/data/tipodegasto';

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
LOCATION '/data/venta';

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
LOCATION '/data/canaldeventa';

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
LOCATION '/data/cliente';

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
LOCATION '/data/producto';

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
LOCATION '/data/empleado';

DROP TABLE IF EXISTS sucursal;
CREATE EXTERNAL TABLE IF NOT EXISTS sucursal (
	IdSucursal		INTEGER,
	Sucursal	VARCHAR(40),
	Domicilio	VARCHAR(150),
	Localidad	VARCHAR(80),
	Provincia	VARCHAR(50),
	Latitud		FLOAT,
	Longitud	FLOAT
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    'separatorChar'=';',
    'skip.header.line.count'='1'
)
LOCATION '/data/sucursal';

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
LOCATION '/data/calendario';

DROP TABLE IF EXISTS proveedor;
CREATE EXTERNAL TABLE IF NOT EXISTS proveedor (
	IdProveedor		INTEGER,
	Nombre			VARCHAR(40),
	Direccion		VARCHAR(150),
	Ciudad			VARCHAR(80),
	Provincia		VARCHAR(50),
	Pais			VARCHAR(20),
	Departamento	VARCHAR(50)
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    'separatorChar'=',',
    'skip.header.line.count'='1'
)
LOCATION '/data/proveedor';