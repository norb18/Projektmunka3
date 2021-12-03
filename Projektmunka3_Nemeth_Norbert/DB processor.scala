// Databricks notebook source
// MAGIC %sql
// MAGIC CREATE TABLE Stocktable4 (
// MAGIC Name STRING,
// MAGIC Current_Price Float,
// MAGIC Delta Float,
// MAGIC Delta_perc Float,
// MAGIC Previous_close Float
// MAGIC )
// MAGIC USING delta
// MAGIC PARTITIONED BY (Name)
// MAGIC LOCATION '/mnt/projekt3/stocktable4'

// COMMAND ----------

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming._
import org.apache.spark.sql.types._

import spark.implicits._
case class DeviceData(Name: String,Current_Price: Float, Delta: Float, Delta_perc: Float,Previous_close: Float )

// COMMAND ----------

import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
val spark = SparkSession
  .builder
  .appName("Projekt3")
  .getOrCreate()

// COMMAND ----------

spark()

// COMMAND ----------

val df = spark.readStream.format("socket").option("host","localhost").option("port",12349).load()
val dff = df.as[String].map(line =>line.split(","))
val dfff = dff.map(line =>DeviceData(line(0),line(1).toFloat,line(2).toFloat,line(3).toFloat,line(4).toFloat))
val queryy = dfff.writeStream.format("delta").trigger(Trigger.ProcessingTime("1 minutes")).option("checkpointLocation", "/mnt/projekt3/stocktable4/_checkpoints/etl-from-json").start("/mnt/projekt3/stocktable4").awaitTermination()

// COMMAND ----------

