package com.huawei.dli;

import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.JsonParser;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.dis.*;
import org.apache.spark.streaming.*;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by wangfei on 2018/8/21.
 */
public class DisToDwsJava {
  Logger log = LoggerFactory.getLogger("DisToDwsJava");

  public static void main(String args[]) {
    SparkConf sparkConf = new SparkConf().setAppName("DisToDwsJava");
    JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, new Duration(10000));
    String endpoint = "https://dis.cn-north-1.myhwclouds.com:20004";
    String region = "cn-north-1";
    String ak = "xx";
    String sk = "yy";
    String projectId = "zz";
    String streamName = "name";
    String startingOffsets = "LATEST";
    String duration = "5";
    String dwsurl = "jdbc:postgresql://veolia-dws.dws.myhuaweiclouds.com:8000/postgres";
    String username = "dbadmin";
    String dwspassword = "password123456";
    String driver = "org.postgresql.Driver";

    Map<String, String> para = new HashMap<>();
    para.put(DISUtils$.MODULE$.PROPERTY_ENDPOINT(), endpoint);
    para.put(DISUtils$.MODULE$.PROPERTY_REGION_ID(), region);
    para.put(DISUtils$.MODULE$.PROPERTY_AK(), ak);
    para.put(DISUtils$.MODULE$.PROPERTY_SK(), sk);
    para.put(DISUtils$.MODULE$.PROPERTY_PROJECT_ID(), projectId);


  }
}
