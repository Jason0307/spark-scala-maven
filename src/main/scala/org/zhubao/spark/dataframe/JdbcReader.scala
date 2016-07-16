package org.zhubao.spark.dataframe

import org.zhubao.spark.SparkUtil
import org.apache.spark.sql.SQLContext
import java.util.Properties

object JdbcReader extends SparkUtil {
  def main(args: Array[String]): Unit = {
    val sc = getInstance()
    val sqlContext = new SQLContext(sc)
    val properties = new Properties()
    properties.put("user", "jasonzhu")
    properties.put("password", "123456")
    
    val df = sqlContext.read.jdbc("jdbc:mysql://127.0.0.1:3306/platform", "tag", properties)
    df.show()
  }
}