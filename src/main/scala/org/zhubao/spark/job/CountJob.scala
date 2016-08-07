package org.zhubao.spark.job

import java.net.URI
import java.util.Properties
import java.util.Random

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.launcher.SparkLauncher

import com.cloudera.livy.LivyClientBuilder
import com.cloudera.livy.scalaapi.LivyScalaClient
import com.cloudera.livy.scalaapi.ScalaJobContext
import java.io.File
import scala.concurrent.Await
import org.scalatest.concurrent.PatienceConfiguration.Timeout
import scala.concurrent.duration.Duration
import java.util.concurrent.TimeUnit

object CountJob {
  def createConf(local: Boolean): Properties = {
    val conf = new Properties
    if (local) {
      conf.put("client.auth.secret", "true")
      conf.put(SparkLauncher.SPARK_MASTER, "local")
      conf.put("spark.app.name", "SparkClientSuite Local App")
    } else {
      val classpath: String = System.getProperty("java.class.path")
      conf.put("spark.app.name", "SparkClientSuite Remote App")
      conf.put(SparkLauncher.DRIVER_MEMORY, "512m")
      conf.put(SparkLauncher.DRIVER_EXTRA_CLASSPATH, classpath)
      conf.put(SparkLauncher.EXECUTOR_EXTRA_CLASSPATH, classpath)
    }
    conf
  }

  def simpleSparkJob(context: ScalaJobContext): Long = {
    val r = new Random
    val count = 5
    val partitions = Math.min(r.nextInt(10) + 1, count)
    val buffer = new ArrayBuffer[Int]()
    for (a <- 1 to count) {
      buffer += r.nextInt()
    }
    println("work here")
    context.sc.parallelize(buffer, partitions).count()
  }

  def main(args: Array[String]): Unit = {
    val conf = createConf(false)
    val client = new LivyScalaClient(new LivyClientBuilder(false).setURI(new URI("http://192.168.99.100:8998")).setAll(conf).build())
    client.uploadJar(new File("target/spark-0.0.1-SNAPSHOT.jar"))
    val sFuture = client.submit(CountJob.simpleSparkJob)
    val result = Await.result(sFuture, Duration(40, TimeUnit.SECONDS))
    println(result)
  }
}