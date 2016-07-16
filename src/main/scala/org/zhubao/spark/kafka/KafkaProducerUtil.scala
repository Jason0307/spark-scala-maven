package org.zhubao.spark.kafka

import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.streaming.Milliseconds
import org.apache.spark.streaming.StreamingContext
import org.zhubao.spark.SparkUtil

import kafka.producer.KeyedMessage
import kafka.producer.ProducerConfig
import kafka.serializer.StringEncoder
import kafka.producer.Producer

object KafkaProducerUtil extends SparkUtil {
  def getProducerConfig(brokerAddr: String): Properties = {
    val props = new Properties()
    props.put("metadata.broker.list", brokerAddr)
    props.put("serializer.class", classOf[IteblogEncoder[Person]].getName)
    props.put("key.serializer.class", classOf[StringEncoder].getName)
    props
  }

  def sendMessages(topic: String, messages: List[Person], brokerAddr: String) {
    val producer = new Producer[String, Person](new ProducerConfig(getProducerConfig(brokerAddr)))
    producer.send(messages.map {
      new KeyedMessage[String, Person](topic, "Iteblog", _)
    }: _*)
    producer.close()
  }

  def main(args: Array[String]) {
    val sparkConf = getInstance()
    val ssc = new StreamingContext(sparkConf, Milliseconds(500))
    val topic = "tracking_person"
    val brokerAddr = "127.0.0.1:9092"

    val data = List(Person("wyp", 23), Person("spark", 34), Person("kafka", 23),
      Person("iteblog", 23))
    sendMessages(topic, data, brokerAddr)
  }
}

case class Person(name: String, age: Int)
