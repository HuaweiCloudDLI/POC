package com.huawei.dli

import java.sql.{Connection, Driver, DriverManager}
import java.util

import com.google.gson.JsonParser
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dis.{ConsumerStrategies, DISUtils}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.slf4j.LoggerFactory

case class DwsInfo(url: String, userName: String, password: String, driver: String)

object DisToDwsKMByOGG {
  val log = LoggerFactory.getLogger("DisToDwsKM")

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("Spark streaming DIS example")

    log.info("Start DIS Spark Streaming demo.")

    //
    val (endpoint, region, ak, sk, projectId, streamName, startingOffsets, duration)
    = ("https://dis.cn-north-1.myhwclouds.com:20004", "cn-north-1",
      "XXX", "XXX", "769b7a1e32ea43b3ab8362070556d81c",
      "dis-veolia", "LATEST", "10")

    val (dwsurl, username, dwspassword, driver) = ("jdbc:postgresql://veolia-dws.dws.myhuaweiclouds.com:8000/postgres",
      "dbadmin", "XXX", "org.postgresql.Driver")


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
          rdd.foreachPartition(savePartitionToDws(DwsInfo(dwsurl, username, dwspassword, driver)))
    }

    //start the computation
    ssc.start()
    ssc.awaitTermination()
  }

  def savePartitionToDws(dwsInfo: DwsInfo) (iter: Iterator[String]): Unit = {
    var connection: Connection = null
    Class.forName(dwsInfo.driver)
    // val property = new Property
    try {
      connection = DriverManager.getConnection(dwsInfo.url, dwsInfo.userName, dwsInfo.password)
      val statement = connection.createStatement()
      val sql = getBatchInsertSql(iter)
      statement.execute(sql)
      statement.close
    } finally {
      if (connection != null) connection.close()
    }
  }

  def getBatchInsertSql(iter: Iterator[String]): String = {
    val parser = new JsonParser
    val stringBuilder = new StringBuilder
    stringBuilder.append("insert into dbadmin.sample_tbl values ")
    var hasRecord = false
    while (iter.hasNext) {
      try {
        hasRecord = true
        val message = iter.next()
        val obj = parser.parse(message).getAsJsonObject
        val equId = obj.get("equId").getAsString
        val equType = obj.get("equType").getAsInt
        val zoneId = obj.get("zoneId").getAsInt
        val uploadTime = obj.get("uploadTime").getAsBigInteger
        val runningTime = obj.get("runningTime").getAsDouble
        val temperature = obj.get("temperature").getAsDouble
        val pressure = obj.get("pressure").getAsDouble
        val waterLine = obj.get("waterLine").getAsDouble
        val targetTemperature = obj.get("targetTemperature").getAsDouble
        val targetPressure = obj.get("targetPressure").getAsDouble
        val targetWaterLine = obj.get("targetWaterLine").getAsDouble
        val feedWater = obj.get("feedWater").getAsDouble
        val noxEmissions = obj.get("noxEmissions").getAsDouble
        val unitLoad = obj.get("unitLoad").getAsDouble
        val fieldSeq = Seq(equId, equType, zoneId, uploadTime, runningTime, temperature, pressure, waterLine,
          targetTemperature, targetPressure, targetWaterLine, feedWater, noxEmissions, unitLoad)
        stringBuilder.append(fieldSeq.mkString("(", ",", "),"))
      } catch {
        case e: Exception =>
          log.error("Parse message failed!", e)
      }
    }
    if (hasRecord) {
      stringBuilder.toString.stripSuffix(",") + ";"
    } else {
      ""
    }
  }


  /**
    * ogg msg is as follow:
    *
  {
    "table":"QASOURCE.TCUSTORD",
    "op_type":"I",
    "op_ts":"2015-11-05 18:45:36.000000",
    "current_ts":"2016-10-05T10:15:51.267000",
    "pos":"00000000000000002928",
    "after":{
        "CUST_CODE":"WILL",
        "ORDER_DATE":"1994-09-30:15:33:00",
        "PRODUCT_CODE":"CAR",
        "ORDER_ID":144,
        "PRODUCT_PRICE":17520.00,
        "PRODUCT_AMOUNT":3,
        "TRANSACTION_ID":100
    }
}
{
    "table":"QASOURCE.TCUSTORD",
    "op_type":"U",
    "op_ts":"2015-11-05 18:45:39.000000",
    "current_ts":"2016-10-05T10:15:51.310002",
    "pos":"00000000000000004300",
    "before":{
        "CUST_CODE":"BILL",
        "ORDER_DATE":"1995-12-31:15:00:00",
        "PRODUCT_CODE":"CAR",
        "ORDER_ID":765,
        "PRODUCT_PRICE":15000.00,
        "PRODUCT_AMOUNT":3,
        "TRANSACTION_ID":100
    },
    "after":{
        "CUST_CODE":"BILL",
        "ORDER_DATE":"1995-12-31:15:00:00",
        "PRODUCT_CODE":"CAR",
        "ORDER_ID":765,
        "PRODUCT_PRICE":14000.00
    }
}
{
    "table":"QASOURCE.TCUSTORD",
    "op_type":"D",
    "op_ts":"2015-11-05 18:45:39.000000",
    "current_ts":"2016-10-05T10:15:51.312000",
    "pos":"00000000000000005272",
    "before":{
        "CUST_CODE":"DAVE",
        "ORDER_DATE":"1993-11-03:07:51:35",
        "PRODUCT_CODE":"PLANE",
        "ORDER_ID":600,
        "PRODUCT_PRICE":135000.00,
        "PRODUCT_AMOUNT":2,
        "TRANSACTION_ID":200
    }
}
{
    "table":"QASOURCE.TCUSTORD",
    "op_type":"T",
    "op_ts":"2015-11-05 18:45:39.000000",
    "current_ts":"2016-10-05T10:15:51.312001",
    "pos":"00000000000000005480",
}
    *
    */
  private def parseMessageToRecord(message: String): Seq[Record] = {
    val parser = new JsonParser
    val obj = parser.parse(message).getAsJsonObject
    val tableName = obj.get("table").getAsString
    val opType = obj.get("op_type").getAsString
    tableName match {
      case "A" =>
        opType match {
          case "I"
        }
      case "B" => //todo
    }
    val values = obj.getAsJsonArray("values")

    val list = new util.ArrayList[Record]
    for (i <- 0 until values.size()) {
      val obj = values.get(i).getAsJsonObject

      val id = obj.get("id").getAsString
      val v = obj.get("v").getAsString
      val q = obj.get("q").getAsString
      val t = obj.get("t").getAsString
      val tagId = getTagId()
      list.add(Record(tagId, v, q, t))
    }
    import scala.collection.JavaConverters._
    list.asScala
  }
}
