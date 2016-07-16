package org.zhubao.spark.kafka

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka.KafkaUtils
import org.zhubao.spark.SparkUtil

import kafka.serializer.StringDecoder
import com.alibaba.fastjson.JSON
import org.apache.spark.SparkConf
import org.apache.spark.streaming.Milliseconds

object KafkaApp extends SparkUtil {
  def main(args: Array[String]): Unit = {
    val topics = "tracking_person"
    val numThreads = 1
    val kafkaParams = Map("zookeeper.connect" -> "127.0.0.1:2181",
      "group.id" -> "test-2",
      "auto.offset.reset" -> "largest")
    val sc = getInstance()
    val ssc = new StreamingContext(sc, Milliseconds(5000))
//    ssc.checkpoint("checkpoint")

    val topicpMap = topics.split(",").map((_, numThreads)).toMap
    val postStream = KafkaUtils.createStream[String, Person, StringDecoder, IteblogDecoder[Person]](ssc, kafkaParams, topicpMap, StorageLevel.MEMORY_ONLY)
    postStream.foreachRDD(rdd => {
      println("rdd : " + rdd)
      val count = rdd.count()
      println("count : " + count)
      if(count > 0) {
        rdd.foreach { person => println(person) }
      }
    })

    ssc.start()
    ssc.awaitTermination()
    ssc.stop()
  }
}