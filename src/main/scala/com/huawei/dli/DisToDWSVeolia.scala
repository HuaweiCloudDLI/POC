package com.huawei.dli

import java.io.StringReader
import java.sql.{Connection, Driver, DriverManager}
import java.util

import com.google.gson.JsonParser
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dis.{ConsumerStrategies, DISUtils}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.postgresql.copy.CopyManager
import org.postgresql.core.BaseConnection
import org.slf4j.LoggerFactory

import scala.util.Random

case class CompanyInfo(companyCode: String, systemCode: String, tagName: String)

case class Record(tagId: String, v: String, q: String, t: String)

object DisToDWSVeolia {

  // val tagMap = Map(CompanyInfo("", "", "100000") -> "100000", CompanyInfo("", "", "100001") -> "100000")
  val tagIdSeq = Seq("100000", "100001", "100002", "100003", "100004", "100005", "100006", "100007", "100008", "100009", "100010")

  val log = LoggerFactory.getLogger("DisToDWSVeolia")

  def main(args: Array[String]): Unit = {

    val sparkConf =
      new SparkConf()
        .setAppName("Spark streaming DIS example")

    log.info("Start DIS Spark Streaming demo.")

    val (endpoint, region, ak, sk, projectId, streamName, startingOffsets, duration)
    = ("https://dis.cn-north-1.myhwclouds.com:20004", "cn-north-1",
      "XXX", "XXXX", "769b7a1e32ea43b3ab8362070556d81c",
      "dis-veolia2", "LATEST", "5")

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
          rdd.flatMap(parseMessageToRecord)
            .foreachPartition(savePartitionToDws(DwsInfo(dwsurl, username, dwspassword, driver)))
    }
//
//    realValue.foreachRDD {
//      rdd =>
//        if (rdd.count() > 0) {
//          rdd.flatMap(parseMessageToRecord).map(record => (record.tagId + record.t, record))
//            .reduceByKey((r1, r2) => Record(r1.tagId, (r1.v.toDouble + r2.v.toDouble).toString, r1.q, r1.t)).map(_._2)
//            .repartition(5)
//            .foreachPartition(savePartitionToDws(DwsInfo(dwsurl, username, dwspassword, driver)))
//        }
//    }

    //start the computation
    ssc.start()
    ssc.awaitTermination()

  }

  def savePartitionToDws(dwsInfo: DwsInfo) (iter: Iterator[Record]): Unit = {
    var connection: Connection = null
    Class.forName(dwsInfo.driver)
    // val property = new Property
    try {
      connection = DriverManager.getConnection(dwsInfo.url, dwsInfo.userName, dwsInfo.password)
      val statement = connection.createStatement()
      val sql = getBatchInsertSql(iter)
      statement.execute(sql)
      statement.close
    } catch {
      case e : Exception =>
        log.info("")
    } finally {
      if (connection != null) connection.close()
    }
  }

  // {"timestamp":1521698375065,"values":[{"id":"SE433.S01.IW1440","v":206,"q":true,"t":1521698358299},{"id":"SE433.S01.LCV1414_ACT","v":42,"q":true,"t":1521698358222}]}
  def getBatchInsertSql(iter: Iterator[Record]): String = {
    val stringBuilder = new StringBuilder
    stringBuilder.append("insert into dbadmin.veolia values ")
    var hasRecord = false
    while (iter.hasNext) {
      try {
        hasRecord = true
        val record = iter.next()
        val fieldSeq = Seq(record.tagId, record.v, record.q, record.t)
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

  // copy dbadmin.veolia from STDIN CSV
  def getBatchCopySql(iter: Iterator[Record]): String = {
    val stringBuilder = new StringBuilder
    var hasRecord = false
    while (iter.hasNext) {
      try {
        hasRecord = true
        val record = iter.next()
        val fieldSeq = Seq(record.tagId, record.v, record.q, record.t)
        stringBuilder.append(fieldSeq.mkString("", ",", "\n"))
      } catch {
        case e: Exception =>
          log.error("Parse message failed!", e)
      }
    }
    stringBuilder.toString
  }

  def copyPartitionToDws(dwsInfo: DwsInfo) (iter: Iterator[Record]): Unit = {
    var connection: Connection = null
    Class.forName(dwsInfo.driver)
    // val property = new Property
    try {
      connection = DriverManager.getConnection(dwsInfo.url, dwsInfo.userName, dwsInfo.password)
      val tuples = getBatchCopySql(iter)
      val cm = new CopyManager(connection.asInstanceOf[BaseConnection])
      val sql = "copy dbadmin.veolia from STDIN CSV"
      cm.copyIn(sql, new StringReader(tuples))
      connection.commit()
    } finally {
      if (connection != null) connection.close()
    }
  }

  def parseMessageToRecord(message: String): Seq[Record] = {
    val parser = new JsonParser
    val obj = parser.parse(message).getAsJsonObject
    val timestamp = obj.get("timestamp").getAsString
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

  private def getTagId(): String = {
    tagIdSeq(Random.nextInt(tagIdSeq.length))
  }

  // "id": "Companycode_protocalname.SystemCode.Tagname",
  private def resolveCompanyInfo(id: String): CompanyInfo = {
    val arr = id.split(".")
    val companyCode = arr(0).split("_")(0)
    CompanyInfo(companyCode, arr(1), arr(2))
  }

}
