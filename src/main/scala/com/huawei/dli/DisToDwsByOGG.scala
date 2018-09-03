package com.huawei.dli

import java.sql.{Connection, Driver, DriverManager}
import java.util
import java.util.HashMap
import java.util.ArrayList

import scala.collection.JavaConverters._

import com.google.gson.{JsonObject, JsonParser}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dis.{ConsumerStrategies, DISUtils}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.slf4j.LoggerFactory

case class DwsInfo(url: String, userName: String, password: String, driver: String)

case class OpRecord(opType: String, key: String, row: String)

object DisToDwsByOGG {

  val log = LoggerFactory.getLogger("DisToDwsOGG")

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("Spark streaming DIS example")

    log.info("Start DIS Spark Streaming demo.")

    val (endpoint, region, ak, sk, projectId, streamName, startingOffsets, duration, batchSize)
    = (args(0), args(1), args(2), args(3), args(4), args(5), args(6), args(7), args(8).toInt)

    // ("jdbc:postgresql://veolia-dws.dws.myhuaweiclouds.com:8000/postgres", "dbadmin", "XXX")
    val (dwsurl, username, dwspassword) = (args(8), args(9), args(10))


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

    realValue.foreachRDD {
      rdd =>
          rdd.foreachPartition(iter => processOggMsgs(batchSize, iter, DwsInfo(dwsurl, username, dwspassword, driver)))
    }

    //start the computation
    ssc.start()
    ssc.awaitTermination()
  }

  def processOggMsgs(batchSize: Int, iter: Iterator[String], dwsInfo: DwsInfo): Unit = {
    val parser = new JsonParser
    var msg = ""
    var tableName = ""
    var opType = ""
    val tableRecords = new HashMap[String, ArrayList[OpRecord]]()
    val INSERT = "I"
    val UPDATE = "U"
    val DELETE = "D"
    var connection: Connection = null
    Class.forName(dwsInfo.driver)
    try {
      connection = DriverManager.getConnection(dwsInfo.url, dwsInfo.userName, dwsInfo.password)
      while (iter.hasNext) {
        msg = iter.next()
        val obj = parser.parse(msg).getAsJsonObject
        tableName = obj.get("table").getAsString
        opType = obj.get("op_type").getAsString
        opType match {
          case INSERT =>
            val list = Option(tableRecords.get(tableName)).map { list =>
              val after = obj.get("after").asInstanceOf[JsonObject].entrySet().iterator().asScala.toSeq
              val keyRecord = after.head
              if (list.asScala.head.opType == INSERT) {
                // every batch we do the insert
                if (list.size() > batchSize) {
                  val sql = s"insert into table $tableName values ${list.asScala.map(_.row).mkString(",")}"
                  val statement = connection.createStatement()
                  statement.execute(sql)
                  statement.close()
                  new util.ArrayList[OpRecord]()
                } else {
                  list.add(OpRecord(INSERT, keyRecord.getKey, after.map(_.getValue).mkString("(", ",", ")")))
                  list
                }
              } else {
                // this case must be delete case
                val sql = s"delete from $tableName ${keyRecord.getKey} in ${list.asScala.map(_.row).mkString("(", ",", ")")}"
                val statement = connection.createStatement()
                statement.execute(sql)
                statement.close()
                new util.ArrayList[OpRecord]()
              }
            }.getOrElse(new util.ArrayList[OpRecord]())
            tableRecords.put(tableName, list)
          case DELETE =>
            val list = Option(tableRecords.get(tableName)).map { list =>
              val after = obj.get("after").asInstanceOf[JsonObject].entrySet().iterator().asScala.toSeq
              val keyRecord = after.head
              if (list.asScala.head.opType == DELETE) {
                // every batch we do the insert
                if (list.size() > batchSize) {
                  val sql = s"delete from $tableName ${keyRecord.getKey} in ${list.asScala.map(_.row).mkString("(", ",", ")")}"
                  val statement = connection.createStatement()
                  statement.execute(sql)
                  statement.close()
                  new util.ArrayList[OpRecord]()
                } else {
                  list.add(OpRecord(INSERT, keyRecord.getKey, keyRecord.getValue.getAsString))
                  list
                }
              } else {
                // this case must be insert
                val sql = s"insert into table $tableName values ${list.asScala.map(_.row).mkString(",")}"
                val statement = connection.createStatement()
                statement.execute(sql)
                statement.close()
                new util.ArrayList[OpRecord]()
              }
            }.getOrElse(new util.ArrayList[OpRecord]())
            tableRecords.put(tableName, list)
          case UPDATE =>
            val after = obj.get("after").asInstanceOf[JsonObject].entrySet().iterator().asScala.toSeq.map(_.toString)
            val sql = s"update $tableName set ${after.mkString(",")} where ${after.head}"
            val statement = connection.createStatement()
            statement.execute(sql)
            statement.close()
        }
      }
    } catch {
      case e : Exception =>
        log.info(s"insert/update/delete table failed: ", e)
    } finally {
      if (connection != null) connection.close()
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
}
