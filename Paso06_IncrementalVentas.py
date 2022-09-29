from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *

from pyspark.sql import SparkSession

spark = SparkSession.\
        builder.\
        appName("prueba-pyspark").\
        master("spark://spark-master:7077").\
        config("spark.executor.memory", "512m").\
        getOrCreate()
        
venta = spark.read.option("compression.codec", "snappy").option("mergeSchema", "true").parquet("hdfs://namenode:9000/data2/venta_sin_outliers")

venta_nvo = spark.read.csv(path="hdfs://namenode:9000/data_nvo/venta_nvo", inferSchema=True, sep=",", header=True)

venta_nvo = venta_nvo.withColumn("Fecha",col("Fecha").cast("date"))
venta_nvo = venta_nvo.withColumn("Fecha_Entrega",col("Fecha_Entrega").cast("date"))
venta_nvo = venta_nvo.withColumn("Precio",col("Precio").cast("float"))
venta_nvo = venta_nvo.withColumn("IdVenta", lit(0))

venta_nvo = venta_nvo.withColumnRenamed("Fecha", "fecha")
venta_nvo = venta_nvo.withColumnRenamed("Fecha_Entrega", "fecha_entrega")
venta_nvo = venta_nvo.withColumnRenamed("IdCanal", "idcanal")
venta_nvo = venta_nvo.withColumnRenamed("IdCliente", "idcliente")
venta_nvo = venta_nvo.withColumnRenamed("IdSucursal", "idsucursal")
venta_nvo = venta_nvo.withColumnRenamed("IdEmpleado", "idempleado")
venta_nvo = venta_nvo.withColumnRenamed("IdProducto", "idproducto")
venta_nvo = venta_nvo.withColumnRenamed("Precio", "precio")
venta_nvo = venta_nvo.withColumnRenamed("Cantidad", "cantidad")
venta_nvo = venta_nvo.withColumnRenamed("IdVenta", "idventa")

venta_nvo = venta_nvo.select("idventa","fecha","fecha_entrega","idcanal","idcliente","idsucursal","idempleado","idproducto","precio","cantidad")

venta = venta.union(venta_nvo)

venta.repartition(1).write.option("compression.codec", "snappy").option("mergeSchema", "true").parquet("hdfs://namenode:9000/data2/venta_incremental")