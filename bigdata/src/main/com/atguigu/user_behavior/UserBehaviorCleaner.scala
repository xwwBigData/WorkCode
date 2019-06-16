package com.atguigu.user_behavior

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 用户行为数据清洗
  * 1、验证数据格式是否正确，切分后长度必须为17
  * 2、手机号脱敏，格式为123xxxx4567
  * 3、去掉username中带有的\n，否则导致写入HDFS时会换行
  */
object UserBehaviorCleaner {
  def main(args: Array[String]): Unit = {
    //防御 判断
    if (args.length != 2) {
      println("Usage:please input inputPath and outputPath")
      System.exit(1)

    }

    // 获取输入输出路径
    val inputPath = args(0)
    val outputPath = args(1)

    val conf = new SparkConf().setAppName(this.getClass.getSimpleName)
    val sc = new SparkContext(conf)

    /**
      * uid STRING comment "用户唯一标识",
      * username STRING comment "用户昵称",
      * gender STRING comment "性别",
      * level TINYINT comment "1代表小学，2代表初中，3代表高中",
      * is_vip TINYINT comment "0代表不是会员，1代表是会员",
      * os STRING comment "操作系统:os,android等",
      * channel STRING comment "下载渠道:auto,toutiao,huawei",
      * net_config STRING comment "当前网络类型",
      * ip STRING comment "IP地址",
      * phone STRING comment "手机号码",
      * video_id INT comment "视频id",
      * video_length INT comment "视频时长，单位秒",
      * start_video_time BIGINT comment "开始看视频的时间缀，秒级",
      * end_video_time BIGINT comment "退出视频时的时间缀，秒级",
      * version STRING comment "版本",
      * event_key STRING comment "事件类型",
      * event_time BIGINT comment "事件发生时的时间缀，秒级"
      *
      */

    // 通过输入路径获取RDD
    val eventRDD: RDD[String] = sc.textFile(inputPath)

    // 清洗数据，在算子中不要写大量业务逻辑，应该将逻辑封装到方法中
    eventRDD.filter(event => checkEventValid(event)) // 验证数据有效性
      .map(event => maskPhone(event)) // 手机号脱敏
      .map(event => repairUsername(event)) // 修复username中带有\n导致的换行
      .coalesce(3)
      .saveAsTextFile(outputPath)

    sc.stop()
  }

  /**
    * username为用户自定义的，里面有要能存在"\n"，导致写入到HDFS时换行
    *
    * @param event
    */
  def repairUsername(event: String) = {
    val fields = event.split("\t")

    // 取出用户昵称
    val username = fields(1)

    // 用户昵称不为空时替换"\n"
    if (username != null && !"".equals(username)) {
      fields(1) = username.replace("\n", "")
    }

    fields.mkString("\t")
  }

  /**
    * 脱敏手机号
    *
    * @param event
    */
  def maskPhone(event: String): String = {
    var maskPhone = new StringBuilder
    val fields: Array[String] = event.split("\t")

    // 取出手机号
    val phone = fields(9)

    // 手机号不为空时做掩码处理
    if (phone != null && !"".equals(phone)) {
      maskPhone = maskPhone.append(phone.substring(0, 3)).append("xxxx").append(phone.substring(7, 11))
      fields(9) = maskPhone.toString()
    }

    fields.mkString("\t")
  }


  /**
    * 验证数据格式是否正确，只有切分后长度为17的才算正确
    *
    * @param event
    */
  def checkEventValid(event: String) = {
    val fields = event.split("\t")
    fields.length == 17
  }
}
