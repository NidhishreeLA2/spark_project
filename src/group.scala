



import org.apache.spark.sql.SparkSession

import org.apache.spark.sql.functions._

import org.apache.spark.sql.Column

import org.apache.spark.sql.types._

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType,LongType};
object SparkReadFile {

   def main(args: Array[String]) {
    val sqlContext = new org.apache.spark.sql.SQLContext(sc);

    import sqlContext.implicits._

val schema = StructType(Array(StructField("rownum",IntegerType,true),StructField("qid",IntegerType,true),StructField("userid",IntegerType,true),StructField("qscore",IntegerType,true),StructField("qtime",LongType,true),StructField("tag",StringType,true),StructField("qvc",IntegerType,true),StructField("qac",IntegerType,true),StructField("aid",IntegerType,true),StructField("uida",IntegerType,true),StructField("as",IntegerType,true),StructField("at",LongType,true)))



val df2 =sqlContext.read.format("com.databricks.spark.csv").schema(schema).option("header", "true").option("delimiter", "|").load("/user/maria_dev/data/project.txt");

df2.select(explode(split(col("tag"),","))).groupBy("col").count().orderBy(col("count").desc).show(10)

df2.registerTempTable("social")
val sql1=spark.sql("select avg(timetaken)/3600 from (select (at - qtime) as timetaken from social)")
sql1.show()

val sql2=spark.sql("select count(qid) from (select (at - qtime) as timetaken,* from social) where timetaken <= 3600")
sql2.show()

val sql3 = spark.sql("select tag from (select (at - qtime) as timetaken,* from social) where timetaken <= 3600")
sql3.show()}}
