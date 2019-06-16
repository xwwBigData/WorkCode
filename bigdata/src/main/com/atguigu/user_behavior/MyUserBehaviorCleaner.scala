package com.atguigu.user_behavior

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object MyUserBehaviorCleaner {
  def main(args: Array[String]): Unit = {
    if (args.length != 2){
      println("Usage:please input inputPath and outputPath")
      System.exit(1)

      val inputpath = args(0)
      val outputpath = args(1)

      val conf: SparkConf = new SparkConf().setAppName(this.getClass.getSimpleName)
      val sc = new SparkContext(conf)
      val eventRDD: RDD[String] = sc.textFile(inputpath)
      eventRDD.filter(event=>checkEventValid(event))
        .map(event=>markPhone(event))
        .map(event=>repairUsername(event))
        .coalesce(3)
        .saveAsTextFile(outputpath)
    }


  }

  //手机号脱敏
  def markPhone(event: String): String = {
    val maskPhone = new StringBuilder

    val fields: Array[String] = event.split("\t")
    val phone: String = fields(9)

    if(phone != null && !" ".equals(phone)){
      val phoneNum: StringBuilder = maskPhone.append(phone.substring(0, 3)).append("xxxx").append(phone.substring(6, 4))
      fields(9) = phoneNum.toString()
    }
    fields.mkString("\t")
  }


  //判空

  // 验证数据有效性
  def checkEventValid(event: String) : Boolean={
    val fields: Array[String] = event.split(" ")
    fields.length == 17
  }

  // 修复username中带有\n导致的换行
  def repairUsername(event: String): String = {
    val fields = event.split("\t")
    // 取出用户昵称
    val username = fields(1)

    if(username != null && !" ".equals(username)){
      val name: String = username.replace("\n","")
      fields(1) = name
    }
    fields.mkString("\t")
  }
}

