package org.zhubao.spark.kafka

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.Seconds

object KafkaSteamApp {
  def main(args: Array[String]): Unit = {
    val zkQuorum = "127.0.0.1:2181"
    val group = "test-group"
    val topics = "tracking_person"
    val numThreads = 5

    val sparkConf = new SparkConf().setAppName("KafkaWordCount").setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf, Seconds(2))
    ssc.checkpoint("checkpoint")

    val topicpMap = topics.split(",").map((_, numThreads)).toMap
    
    val postStream = KafkaUtils.createStream(ssc, zkQuorum, group, topicpMap).map(_._2)
    postStream.foreachRDD((rdd, time) => {
      val count = rdd.count()
      println("count : " + count)
      if (count > 0) {
         println("rdd : " + rdd)
         rdd.foreach { x => println(x)}
      }
    })

    ssc.start()
    ssc.awaitTermination()
  }
}