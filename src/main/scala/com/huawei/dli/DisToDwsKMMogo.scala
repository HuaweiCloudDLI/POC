package com.huawei.dli

import java.sql.{Connection, Driver, DriverManager}
import java.util

import scala.collection.JavaConverters._

import com.google.gson.{JsonObject, JsonParser}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dis.{ConsumerStrategies, DISUtils}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.slf4j.LoggerFactory

case class DwsInfo(url: String, userName: String, password: String, driver: String)

object DisToDwsKMMogo {
  val log = LoggerFactory.getLogger("DisToDwsKMMogo")

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("Spark streaming DIS example DisToDwsKMMogo")

    log.info("Start DIS Spark Streaming DisToDwsKMMogo demo.")

    val ak = args(0)
    val sk = args(1)
    val streamName = args(2)
    val projectId = args(3)
    val dwsurl = args(4)
    val userName = args(5)
    val password = args(6)

    val (endpoint, region, startingOffsets, duration)
    = (
      "https://dis.cn-north-1.myhwclouds.com:20004",
      "cn-north-1",
      "LATEST",
      "10")

    val driver = "org.postgresql.Driver"


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

    realValue.foreachRDD { rdd =>
      rdd.foreachPartition(iter => processIter(iter, DwsInfo(dwsurl, userName, password, driver)))
    }

    //start the computation
    ssc.start()
    ssc.awaitTermination()
  }

  private def processIter(iter: Iterator[String], dwsInfo: DwsInfo): Unit = {
    var preOp = "unknown"
    var op = "unknown"
    val parser = new JsonParser
    var obj: JsonObject = null
    val list = new java.util.ArrayList[JsonObject]()
    while (iter.hasNext) {
      obj = parser.parse(iter.next()).getAsJsonObject
      val collection = obj.get("ns").getAsString
      if (collection == "km_lb.pathologyResultMongoEntity") {
        op = obj.get("op").getAsString
        op match {
          case "i" if Set("i", "unknown").contains(preOp) =>
            list.add(obj.get("o").getAsJsonObject)
          case "i" if preOp == "d" =>
          // handle delete object list
          case "d" if Set("d", "unknown").contains(preOp) =>
            list.add(obj.get("o").getAsJsonObject)
          case "d" if preOp == "u" =>
          // handle insert object list
          case "u" =>
          // handle update object list
        }

        preOp = op
      }
    }
  }

  private def handleDelete(list: util.List[JsonObject], dwsInfo: DwsInfo): Unit = {
    val idlist = list.asScala.map(_.get("_id").getAsJsonObject.toString).map(x => s"'$x'").mkString("(", ",", ")")
    val sql = s"delete from dw_kingmed.lb_task_detail where id in $idlist"
    println(s"delete: $sql")
    var connection: Connection = null
    Class.forName(dwsInfo.driver)
    // val property = new Property
    try {
      connection = DriverManager.getConnection(dwsInfo.url, dwsInfo.userName, dwsInfo.password)
      val statement = connection.createStatement()
      statement.execute(sql)
      statement.close
    } finally {
      if (connection != null) connection.close()
    }
  }

  private def handleInsert(list: util.List[JsonObject], dwsInfo: DwsInfo): Unit = {
    val stringBuilder = new StringBuilder
    stringBuilder.append("insert into dw_kingmed.lb_task_detail values ")
    var hasRecord = false
    val iter = list.iterator()
    while (iter.hasNext) {
      try {
        hasRecord = true
        val obj = iter.next()
        val fieldSeq = Seq(
          obj.get("_id").getAsJsonObject.toString,
          Option(obj.get("gynecoCytologyResult")).map(_.getAsJsonObject.toString).getOrElse("null"),
          Option(obj.get("taskId")).map(_.getAsString).getOrElse("null"),
          Option(obj.get("experimentNo")).map(_.getAsString).getOrElse("null"),
          Option(obj.get("applicationId")).map(_.getAsString).getOrElse("null"),
          Option(obj.get("testItemCode")).map(_.getAsString).getOrElse("null"),
          obj.get("_class").getAsString
        )
        stringBuilder.append(fieldSeq.mkString("(", ",", "),"))
      } catch {
        case e: Exception =>
          log.error("Parse message failed!", e)
      }
    }
    if (hasRecord) {
      val sql = stringBuilder.toString.stripSuffix(",") + ";"
      println(s"insert: $sql")
      var connection: Connection = null
      Class.forName(dwsInfo.driver)
      // val property = new Property
      try {
        connection = DriverManager.getConnection(dwsInfo.url, dwsInfo.userName, dwsInfo.password)
        val statement = connection.createStatement()
        statement.execute(sql)
        statement.close
      } finally {
        if (connection != null) connection.close()
      }
    }

  }

  def savePartitionToDws(dwsInfo: DwsInfo)(iter: Iterator[String]): Unit = {
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
    *
    */
  private def parseMessage(message: String): (String, String, String) = {
    val parser = new JsonParser
    val obj = parser.parse(message).getAsJsonObject
    val collection = obj.get("ns").getAsString
    obj.get("op").getAsString match {
      case "i" => (collection, "i", obj.get("o").getAsJsonObject.toString)
      case "u" => (collection, "u", obj.get("o").getAsJsonObject.toString)
      case "d" => (collection, "d", obj.get("o").getAsJsonObject.toString)
    }
  }
}
