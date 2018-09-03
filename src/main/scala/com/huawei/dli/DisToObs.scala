package com.huawei.dli

import java.text.SimpleDateFormat
import java.util.Date

import com.google.gson.JsonParser
import org.apache.hadoop.io.compress.GzipCodec
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dis.{ConsumerStrategies, DISUtils}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.slf4j.LoggerFactory

object DisToObs {

  def main(args: Array[String]): Unit = {


    val sparkConf = new SparkConf().setAppName("Spark streaming DIS example")
    val log = LoggerFactory.getLogger("veolia")
    log.info("Start DIS Spark Streaming demo.")

    val (endpoint, region, ak, sk, projectId, streamName, startingOffsets, duration)
        = ("https://dis.cn-north-1.myhwclouds.com:20004", "cn-north-1",
      "XXX", "XXX", "769b7a1e32ea43b3ab8362070556d81c",
      "dis-veolia2", "LATEST", "10")


    val ssc = new StreamingContext(sparkConf, Seconds(duration.toInt))

    val params = Map(
      DISUtils.PROPERTY_ENDPOINT -> endpoint,
      DISUtils.PROPERTY_REGION_ID -> region,
      DISUtils.PROPERTY_AK -> ak,
      DISUtils.PROPERTY_SK -> sk,
      DISUtils.PROPERTY_PROJECT_ID -> projectId)
    val stream = DISUtils.createDirectStream[String, String](
      ssc,
      ConsumerStrategies.Assign[String, String](streamName, params, startingOffsets))

    val realValue = stream.map(record => record.value.toString)

    realValue.foreachRDD {
      rdd =>
          val timestamp = new SimpleDateFormat("yyyy-MM-dd-hh-mm").format(new Date)
          val timeStampArr = timestamp.split("-")
          val year = timeStampArr(0)
          val month = timeStampArr(1)
          val day = timeStampArr(2)
          val hour = timeStampArr(3)
          val minute = timeStampArr(4)
          rdd.map(appendTimeStamp(_, timestamp)).saveAsTextFile(
            s"s3a://$ak:$sk@obs-veolia/test2/$year/$month/$day/$hour/$minute", classOf[GzipCodec])
    }

    //start the computation
    ssc.start()
    ssc.awaitTermination()

  }

  // timeStamp yyyy-MM-dd-hh-mm
  private def appendTimeStamp(message: String, timeStamp: String): String = {
    val timeStampArr = timeStamp.split("-")
    val year = timeStampArr(0)
    val month = timeStampArr(1)
    val day = timeStampArr(2)
    val hour = timeStampArr(3)
    val minute = timeStampArr(4)

    val parser = new JsonParser
    val obj = parser.parse(message).getAsJsonObject
    obj.addProperty("dli_year", year)
    obj.addProperty("dli_month", month)
    obj.addProperty("dli_day", day)
    obj.addProperty("dli_hour", hour)
    obj.addProperty("dli_minute", minute)
    obj.toString
  }
}
