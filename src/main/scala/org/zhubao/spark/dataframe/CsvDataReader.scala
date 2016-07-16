package org.zhubao.spark.dataframe

import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object CsvDataReader {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("RDD").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    val df = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("delimiter", ";").load("hdfs://127.0.0.1:9000/spark/bank.csv")
    df.show()
  }
}