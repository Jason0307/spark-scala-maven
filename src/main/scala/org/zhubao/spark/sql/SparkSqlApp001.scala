package org.zhubao.spark.sql

import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object SparkSqlApp001 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("RDD").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    org.apache.spark.sql.catalyst.encoders.OuterScopes.addOuterScope(this);
    val df = sqlContext.read.json("hdfs://127.0.0.1:9000/spark/people.json")
    val people = df.as[People]
    people.show()
    df.show()
    df.select(df("age"), df("name")).filter(df("age") > 20).show()
  }
}

case class People(name: String, age: Option[Long])