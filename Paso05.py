from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *
import time

startTimeQuery = time.clock()

from pyspark.sql import SparkSession

spark = SparkSession.\
        builder.\
        appName("prueba-pyspark").\
        master("spark://spark-master:7077").\
        config("spark.executor.memory", "512m").\
        getOrCreate()

venta = spark.read.option("compression.codec", "snappy").option("mergeSchema", "true").parquet("hdfs://namenode:9000/data2/venta")

ventas_out = venta.na.drop(subset=['precio','cantidad']).groupBy("idproducto").agg(mean(venta.precio).alias("promedio"), stddev(venta.precio).alias("stddev"))

ventas_out = ventas_out.withColumn("PrecioMaximo", ventas_out.promedio + ventas_out.stddev * 3).withColumn("PrecioMinimo", ventas_out.promedio - ventas_out.stddev * 3)

ventas_out.repartition(1).write.option("compression.codec", "snappy").option("mergeSchema", "true").parquet("hdfs://namenode:9000/data2/venta_criterio_outliers", mode="overwrite")

venta = venta.alias("v").join(ventas_out.alias("o"), venta['idproducto'] == ventas_out['idproducto']).select("v.idventa","v.fecha","v.fecha_entrega","v.idcanal","v.idcliente","v.idsucursal","v.idempleado","v.idproducto","v.precio","v.cantidad","o.promedio","o.stddev","o.PrecioMaximo","o.PrecioMinimo")

venta.withColumn("PrecioMaximo",col("PrecioMaximo").cast("float"))

venta.withColumn("PrecioMinimo",col("PrecioMinimo").cast("float"))

def detecta_outlier(valor, maximo, minimo):
    return (valor < minimo) or (valor > maximo)

udf_detecta_outlier = udf(lambda valor, MaxLimit, MinLimit: detecta_outlier(valor, MaxLimit, MinLimit), BooleanType())

venta = venta.na.drop(subset=['precio','cantidad'])

venta = venta.withColumn("esOutlier", udf_detecta_outlier(venta.precio, venta.PrecioMaximo, venta.PrecioMinimo)).filter("NOT esOutlier")

venta = venta.select(["idventa","fecha","fecha_entrega","idcanal","idcliente","idsucursal","idempleado","idproducto","precio","cantidad"])

venta.repartition(1).write.option("compression.codec", "snappy").option("mergeSchema", "true").parquet("hdfs://namenode:9000/data2/venta_sin_outliers", mode="overwrite")

endTimeQuery = time.clock()
runTimeQuery = endTimeQuery - startTimeQuery
print("Tiempo de Ejecucion: %d", runTimeQuery)