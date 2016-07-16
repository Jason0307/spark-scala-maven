package org.zhubao.spark

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

class SparkUtil {
  def createSparkContext() : SparkContext = {
    val conf = new SparkConf().setMaster("local").setAppName("App")
    val sc = new SparkContext(conf)
    return sc
  }
}