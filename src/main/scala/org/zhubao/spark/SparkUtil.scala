package org.zhubao.spark

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

class SparkUtil {
  @transient private var instance: SparkContext = _
  private val conf: SparkConf = new SparkConf()
    .setMaster("local[2]")
    .setAppName("myApp")

  def getInstance(): SparkContext = {
    if (instance == null) {
      instance = new SparkContext(conf)
    }
    instance
  }
}