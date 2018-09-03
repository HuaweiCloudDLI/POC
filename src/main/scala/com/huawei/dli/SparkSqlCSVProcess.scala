package com.huawei.dli

import scala.collection.JavaConverters._

import com.google.gson.JsonParser
import org.apache.spark.sql.SparkSession

/**
  * Created by wangfei on 2018/8/21.
  */
object SparkSqlCSVProcess {

  // para: master, inputpath, tableApath, tableBpath, delimiter
  // example:
  // yarn-cluster
  // s3a://jinyu/mogo/pathologyresultmongoentity.txt
  // s3a://jinyu/mogo/A
  // s3a://jinyu/mogo/B
  // #@#@#
  // ak
  // sk

  def main(args: Array[String]): Unit = {
    val master = args(0)
    val inputPath = args(1)
    val tableAPath = args(2)
    val tableBPath = args(3)
    val delimiter = args(4)
    val ak = args(5)
    val sk = args(6)

    val spark = SparkSession
      .builder()
      .appName("Spark SQL Json Example")
      .master(master)
      .getOrCreate()
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", ak)
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", sk)

    // gynecoCytologyResult,taskId,experimentNo,applicationId,testItemList,testItemCode,_class
    val origin = spark
      .sparkContext
      .textFile(inputPath)
    val a = origin.map { line =>
        val x = line.split("\t")
        Array(x(0), x(1), x(2), x(3), x(5), x(6)).mkString(delimiter)
      }
    println(s"######### AAAA ######")
    // a.take(10).foreach(println)

    a.saveAsTextFile(tableAPath)

    val b = origin.flatMap { line =>
        val x = line.split("\t")
        val taskId = x(1)
        val testItemList = x(4)
        val parser = new JsonParser
        val obj = parser.parse(s"{testItemList: $testItemList}").getAsJsonObject
        val itemList = obj.get("testItemList")
        if (itemList.isJsonArray) {
          val array = obj.getAsJsonArray("testItemList")
          if (array.size() > 0) {
            obj.getAsJsonArray("testItemList").iterator().asScala.flatMap { e =>
              val obj = e.getAsJsonObject()
              obj.entrySet().asScala.map { kv =>
                Seq(taskId, kv.getKey, kv.getValue.getAsString).mkString(delimiter)
              }
            }
          } else {
            Iterator.empty
          }
        } else {
          Iterator.empty
        }
      }
    println(s"######### BBBB ######")
    // b.take(10).foreach(println)
    b.saveAsTextFile(tableBPath)
  }
}
