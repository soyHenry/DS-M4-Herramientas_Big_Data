import org.apache.spark::spark-sql:3.0.0
import org.apache.spark.sql._

val startTimeMillis = System.currentTimeMillis()

val spark = SparkSession.
            builder().
            appName("prueba-scala").
            master("spark://spark-master:7077").
            config("spark.executor.memory", "512m").
            getOrCreate()
			
			
case class flightSchema(DayofMonth:String, DayOfWeek:String, Carrier:String, OriginAirportID:String, DestAirportID:String, DepDelay:String, ArrDelay:String)

case class airportsSchema(airport_id:String, city:String, state:String, name:String)

val flights = spark.read.format("csv").option("sep", ",").option("header", "true").load("hdfs://namenode:9000/data/flights/raw-flight-data.csv").as[flightSchema]

var flights2 = flights.dropDuplicates().na.fill(0, Array("ArrDelay", "DepDelay"))

var airports = spark.read.format("csv").option("sep", ",").option("header", "true").load("hdfs://namenode:9000/data/flights/airports.csv").as[airportsSchema]

val flightsByOrigin = flights.join(airports, $"OriginAirportID" === $"airport_id").groupBy("city").count()

flightsByOrigin.repartition(1).write.format("csv").mode("overwrite").option("sep", ",").option("header", "true").save("hdfs://namenode:9000/data/flightsByOrigin-procesado-scala")

val endTimeMillis = System.currentTimeMillis()

val durationMilliSeconds = (endTimeMillis - startTimeMillis)

print("Tiempo de Ejecucion", durationMilliSeconds)