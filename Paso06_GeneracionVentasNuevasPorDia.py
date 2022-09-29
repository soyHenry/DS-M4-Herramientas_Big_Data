from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *

# Completar con la fecha nueva a ingestar:
fecha_nvo = '2021-01-01'

from pyspark.sql import SparkSession

spark = SparkSession.\
        builder.\
        appName("prueba-pyspark").\
        master("spark://spark-master:7077").\
        config("spark.executor.memory", "512m").\
        getOrCreate()

cliente = spark.read.csv(path="hdfs://namenode:9000/data_nvo/Cliente.csv", inferSchema=True, sep=";", header=True)

empleado = spark.read.csv(path="hdfs://namenode:9000/data_nvo/Empleado.csv", inferSchema=True, sep=",", header=True)

producto = spark.read.csv(path="hdfs://namenode:9000/data_nvo/Producto.csv", inferSchema=True, sep=",", header=True)

cliente.createOrReplaceTempView("cliente")

cliente_nvo = spark.sql("SELECT ID AS IdCliente FROM cliente WHERE RAND() < 0.025")
cliente_nvo.createOrReplaceTempView("cliente_nvo")

empleado.createOrReplaceTempView("empleado")

empleado_nvo = spark.sql("SELECT CodigoEmpleado AS IdEmpleado, IdSucursal FROM empleado WHERE RAND() < 0.25")
empleado_nvo.createOrReplaceTempView("empleado_nvo")

producto.createOrReplaceTempView("producto")

producto_nvo = spark.sql("SELECT ID_PRODUCTO as IdProducto, Precio FROM producto WHERE RAND() < 0.2")
producto_nvo.createOrReplaceTempView("producto_nvo")

consulta = "SELECT '" + str(fecha_nvo) + "' AS Fecha, "
consulta = consulta + "ROUND(RAND() * 4 + 1, 0) AS Dias_Entrega, "
consulta = consulta + "ROUND(RAND() * 2 + 1, 0) AS IdCanal, "
consulta = consulta + "c.IdCliente, "
consulta = consulta + "e.IdSucursal, "
consulta = consulta + "e.IdEmpleado, "
consulta = consulta + "p.IdProducto, "
consulta = consulta + "p.Precio, "
consulta = consulta + "ROUND(RAND() * 3 + 1, 0) AS Cantidad "
consulta = consulta + "FROM cliente_nvo c, producto_nvo p, empleado_nvo e "
consulta = consulta + "WHERE RAND() < 0.0005"

venta_nvo = spark.sql(consulta)

venta_nvo = venta_nvo.withColumn("IdCanal",col("IdCanal").cast("int"))
venta_nvo = venta_nvo.withColumn("Cantidad",col("Cantidad").cast("int"))
venta_nvo = venta_nvo.withColumn("Dias_Entrega",col("Dias_Entrega").cast("int"))
venta_nvo = venta_nvo.withColumn("Fecha",col("Fecha").cast("date"))

venta_nvo.createOrReplaceTempView("venta_nvo")

venta_nvo = spark.sql("SELECT 0 AS IdVenta, Fecha, DATE_ADD(Fecha, Dias_Entrega) AS Fecha_Entrega, IdCanal, IdCliente, IdSucursal, IdEmpleado, IdProducto, Precio, Cantidad FROM venta_nvo")

venta_nvo.repartition(1).write.csv(path="hdfs://namenode:9000/data_nvo/venta_nvo", sep=",", header=True, mode="overwrite")