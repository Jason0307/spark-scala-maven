package org.zhubao.spark.dataframe

import org.zhubao.spark.SparkUtil
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions.max
import org.apache.spark.sql.functions.min
import org.apache.spark.sql.functions.mean
import org.apache.spark.sql.types.IntegerType

object DataFrameGroupApp extends SparkUtil{
  def main(args: Array[String]): Unit = {
    val sc = getInstance()
    val sqlContext = new SQLContext(sc)
    var df = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").load("hdfs://127.0.0.1:9000/spark/score.csv")
    df = df.withColumn("scoreInt", df("score").cast(IntegerType)).drop("score").withColumnRenamed("scoreInt", "score");
    
    df.describe("score").show()
    
    println("=====  print max score  =====")
    df.select(max("score")).show()
    
    println("=====  print min score  =====")
    df.select(min("score")).show()
    
    println("=====  print avg score  =====")
    df.select(mean("score")).show()
    
    df.groupBy("class").agg(mean("score")).show()
  }
}