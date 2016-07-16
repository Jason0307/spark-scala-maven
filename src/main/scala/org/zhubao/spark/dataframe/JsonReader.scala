package org.zhubao.spark.dataframe

import org.zhubao.spark.SparkUtil
import org.apache.spark.sql.SQLConf
import org.apache.spark.sql.SQLContext

object JsonReader extends SparkUtil{
  def main(args: Array[String]): Unit = {
    val sc = createSparkContext()
    val sqlContext = new SQLContext(sc)
    val people = sqlContext.read.json("hdfs://127.0.0.1:9000/spark/people.json")
    people.show()
    val filterDf = people.select(people("age"), people("name")).filter(people("age").isNotNull)
    filterDf.show()
  }
}