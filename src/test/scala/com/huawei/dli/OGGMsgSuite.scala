package com.huawei.dli

import scala.collection.JavaConverters._

import com.google.gson.{JsonObject, JsonParser}
import org.scalatest.{BeforeAndAfterAll, FunSuite, Outcome}

/**
  * Created by wangfei on 2018/9/3.
  */
class OGGMsgSuite extends FunSuite with BeforeAndAfterAll {
  test("ogg msg insert") {
    val insertMsg =
      """
        |  {
        |    "table":"QASOURCE.TCUSTORD",
        |    "op_type":"I",
        |    "op_ts":"2015-11-05 18:45:36.000000",
        |    "current_ts":"2016-10-05T10:15:51.267000",
        |    "pos":"00000000000000002928",
        |    "after":{
        |        "CUST_CODE":"WILL",
        |        "ORDER_DATE":"1994-09-30:15:33:00",
        |        "PRODUCT_CODE":"CAR",
        |        "ORDER_ID":144,
        |        "PRODUCT_PRICE":17520.00,
        |        "PRODUCT_AMOUNT":3,
        |        "TRANSACTION_ID":100
        |    }
        |  }
      """.stripMargin

    val parser = new JsonParser
    val obj = parser.parse(insertMsg).getAsJsonObject
    val iter = obj.get("after").asInstanceOf[JsonObject].entrySet().iterator()

    // iter is follow the order of origin msg
    val res = iter.asScala.toSeq.map(_.toString).mkString(",")
    val answer = "CUST_CODE=\"WILL\",ORDER_DATE=\"1994-09-30:15:33:00\",PRODUCT_CODE=\"CAR\",ORDER_ID=144,PRODUCT_PRICE=17520.00,PRODUCT_AMOUNT=3,TRANSACTION_ID=100"
    assert(res == answer, s"res is not correct, ###### $res ######")
  }

  test("sql") {
    val insertMsg =
      """
        |  {
        |    "table":"QASOURCE.TCUSTORD",
        |    "op_type":"I",
        |    "op_ts":"2015-11-05 18:45:36.000000",
        |    "current_ts":"2016-10-05T10:15:51.267000",
        |    "pos":"00000000000000002928",
        |    "after":{
        |        "CUST_CODE":"WILL",
        |        "ORDER_DATE":"1994-09-30:15:33:00",
        |        "PRODUCT_CODE":"CAR",
        |        "ORDER_ID":144,
        |        "PRODUCT_PRICE":17520.00,
        |        "PRODUCT_AMOUNT":3,
        |        "TRANSACTION_ID":100
        |    }
        |  }
      """.stripMargin

    val parser = new JsonParser
    val obj = parser.parse(insertMsg).getAsJsonObject
    val tableName = obj.get("table").getAsString
    val after = obj.get("after").asInstanceOf[JsonObject].entrySet().iterator().asScala.toSeq
    val row = after.map(_.getValue).mkString("(", ",", ")")
    val sql = s"insert into table $tableName values $row"
    val newSQL = sql.replace("\"", "\'")
    println(sql)
    println(newSQL)

  }
}

