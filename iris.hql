ADD JAR hdfs://namenode:9000/tmp/udfs/mongo-java-driver-3.12.11.jar;
ADD JAR hdfs://namenode:9000/tmp/udfs/mongo-hadoop-core-2.0.2.jar;
ADD JAR hdfs://namenode:9000/tmp/udfs/mongo-hadoop-hive-2.0.2.jar;

CREATE EXTERNAL TABLE iris (fila STRING,sepal_length DOUBLE,sepal_width DOUBLE,petal_length DOUBLE,petal_width DOUBLE,species STRING) STORED BY 'com.mongodb.hadoop.hive.MongoStorageHandler' WITH SERDEPROPERTIES('mongo.columns.mapping'='{"fila":"fila","sepal_length":"sepal_length","sepal_width":"sepal_width","petal_length":"petal_length","petal_width":"petal_width","species":"species"}') TBLPROPERTIES('mongo.uri'='mongodb://user:pass@192.168.0.12:27017/datasets.iris');
