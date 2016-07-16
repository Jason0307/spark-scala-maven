package org.zhubao.spark.dataframe

import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object Dataframe001 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("DataFrame")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val banks = sqlContext.read.json("D:/software/bigdata/resources/people.json")
    banks.show()
  }
}