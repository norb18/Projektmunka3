// Databricks notebook source
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._ // not necessary since Spark 1.3
  
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming._
import org.apache.spark.sql.types._
import spark.implicits._

// COMMAND ----------

case class DeviceData(Name: String, Current_Price: Double, Delta: Double,Delta_perc: Double, Previous_close: Double)

// COMMAND ----------

val ssc = new StreamingContext(sc, Seconds(60))
val lines = ssc.socketTextStream("localhost", 12351)

// COMMAND ----------



// COMMAND ----------

val words = lines.map(splitted =>DeviceData(splitted.split(", ")(0),splitted.split(", ")(1).toDouble,splitted.split(", ")(2).toDouble,splitted.split(", ")(3).toDouble,splitted.split(", ")(4).toDouble))
val keyword = words.map(w=>(w.Name,(w.Current_Price,w.Delta,w.Delta_perc,w.Previous_close,1)))
val counts = keyword.reduceByKey((v1,v2)=>(v1._1+v2._1,v1._2+v2._2,v1._3+v2._3,v1._4+v2._4,v1._5+v2._5))
val transformed = counts.map(wds=>(wds._1,wds._2._1/wds._2._5,wds._2._2/wds._2._5,wds._2._5/wds._2._5,wds._2._4/wds._2._5))
val final_data = transformed.map(my_data=>DeviceData(my_data._1,my_data._2,my_data._3,my_data._4,my_data._5))

// COMMAND ----------

val output = final_data.map(w=>w.Name+","+w.Current_Price+","+w.Delta+","+w.Delta_perc+","+w.Previous_close)

// COMMAND ----------

val my_new = output.foreachRDD(rdd =>{ if (!rdd.isEmpty()){rdd.toDF("value").coalesce(1).write.mode("append").format("text").save("file:/tmp/adatok")}})

// COMMAND ----------

ssc.start()

// COMMAND ----------

ssc.stop()

// COMMAND ----------

