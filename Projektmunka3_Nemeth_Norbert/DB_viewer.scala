// Databricks notebook source
// MAGIC %sql
// MAGIC SELECT * FROM Stocktable4;

// COMMAND ----------

// MAGIC %sql
// MAGIC Select * from delta.`/mnt/projekt3/stocktable4`

// COMMAND ----------

spark.sql("select * from tablprobabbb").show(200)

// COMMAND ----------

