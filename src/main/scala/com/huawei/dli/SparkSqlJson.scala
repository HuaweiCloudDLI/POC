package com.huawei.dli

import scala.collection.JavaConverters._

import com.google.gson.JsonParser
import org.apache.spark.sql.SparkSession

/**
  * Created by wangfei on 2018/8/23.
  */
object SparkSqlJson {

  def main(args: Array[String]): Unit = {
    val master = args(0)
    val inputPath = args(1)
    val tableAPath = args(2)
    val tableBPath = args(3)
    val delimiter = args(4)
    val ak = args(5)
    val sk = args(6)

//    val master = "local[4]"
//    val inputPath = "file:////Users/wangfei/Downloads/KM_ENHANCE_FILE_UPLOAD_LOG.txt"
//    val tableAPath = "file:////Users/wangfei/Downloads/A"
//    val tableBPath = "file:////Users/wangfei/Downloads/B"
//    val delimiter = "#@#@#"
//    val ak = "ak"
//    val sk = "sk"

    val spark = SparkSession
      .builder()
      .appName("Spark SQL Json Example")
      .getOrCreate()
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", ak)
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", sk)

    val origin = spark
      .sparkContext
      .textFile(inputPath)
      .map { line =>
        val parser = new JsonParser
        parser.parse(line).getAsJsonObject
      }

    // gynecoCytologyResult,taskId,experimentNo,applicationId,testItemList,testItemCode,_class
    val a = origin.map { obj =>
      Seq(
        Option(obj.get("gynecoCytologyResult")).map(_.getAsJsonObject.toString).getOrElse("null"),
        Option(obj.get("taskId")).map(_.getAsString).getOrElse("null"),
        Option(obj.get("experimentNo")).map(_.getAsString).getOrElse("null"),
        Option(obj.get("applicationId")).map(_.getAsString).getOrElse("null"),
        Option(obj.get("testItemCode")).map(_.getAsString).getOrElse("null"),
        obj.get("_class").getAsString
      ).mkString(delimiter)
    }
    println(s"######### AAAA ######")
    // a.take(10).foreach(println)

    a.saveAsTextFile(tableAPath)

    val b = origin.flatMap { obj =>
      val taskId = Option(obj.get("taskId")).map(_.getAsString).getOrElse("null")
      val itemList = obj.get("testItemList")
      if (itemList != null && itemList.isJsonArray) {
        val array = obj.getAsJsonArray("testItemList")
        if (array.size() > 0) {
          obj.getAsJsonArray("testItemList").iterator().asScala.zipWithIndex.flatMap { case (e, id) =>
            val obj = e.getAsJsonObject()
            obj.entrySet().asScala.map { kv =>
              Seq(taskId, id, kv.getKey, kv.getValue.getAsString).mkString(delimiter)
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
