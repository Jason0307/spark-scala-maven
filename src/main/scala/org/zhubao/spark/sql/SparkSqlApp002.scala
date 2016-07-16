package org.zhubao.spark.sql

import scala.reflect.runtime.universe

import org.apache.spark.sql.SQLContext
import org.zhubao.spark.SparkUtil


object SparkSqlApp002 extends SparkUtil{
  def main(args: Array[String]): Unit = {
    val sc = getInstance();
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    val banks = sc.textFile("hdfs://127.0.0.1:9000/spark/bank.csv")
    val df = banks.map(_.split(";")).filter { x => !x(0).contains("age")}.map(bank => {
      Bank(bank(0).toInt, bank(1), bank(2), bank(3), bank(5).toInt)
    }).toDF
    df.registerTempTable("bank")
    val filterDf = sqlContext.sql("SELECT * FROM bank WHERE age >= 32")
    filterDf.show()
  }
}

case class Bank(age: Int, job: String, marital: String, education: String, balance: Int)