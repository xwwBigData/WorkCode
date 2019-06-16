package com.atguigu.streaming

import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import net.ipip.ipdb.{City, CityInfo}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import scalikejdbc.{ConnectionPool, DB}

object StreamingDemo {

  //获取配置文件中的参数
  private val prop = new Properties()
  prop.load(this.getClass.getClassLoader.getResourceAsStream("VipIncrementAnalysis.properties"))

  val ipdb = new City(this.getClass.getClassLoader.getResourceAsStream("ipipfree.ipdb"))

  val driver = prop.getProperty("jdbcDriver")
  val url = prop.getProperty("jdbcUrl")
  val user = prop.getProperty("jdbcUser")
  val password = prop.getProperty("jdbcPassword")
  val duration = prop.getProperty("processingInterval")
  val brokers = prop.getProperty("brokers")
  val topic = prop.getProperty("topic")

  val sdf = new SimpleDateFormat("yyyy-MM-dd")

  Class.forName(driver)
  ConnectionPool.singleton(url, user, password)


  def main(args: Array[String]): Unit = {
    // TODO 1 参数校验

    if (args.length != 1) {
      println("缺少checkpoint目录的参数")
      System.exit(1)
    }
    val checkpointDir = args(0)


    // TODO 4 创建sparkstreamingcontext
    val ssc: StreamingContext = StreamingContext.getOrCreate(checkpointDir, () => getVipIncrementEveryDay(checkpointDir))

    // TODO 8 ssc开启
    ssc.start()
    ssc.awaitTermination()
  }

  def getVipIncrementEveryDay(checkpointDir: String): StreamingContext = {

    //创建ssc
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName)
    val ssc = new StreamingContext(conf, Seconds(duration.toLong))

    //准备kafka相关参数
    val kafkaParam = Map(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokers)

    //获取偏移量,从mysql获取
    val fromOffset: Map[TopicAndPartition, Long] = DB.readOnly(session => session.list("select topic, part_id, offset from topic_offset") {
      rs => TopicAndPartition(rs.string("topic"), rs.int("part_id")) -> rs.long("offset")
    }.toMap)

    val dstream: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](ssc,
      kafkaParam,
      fromOffset,
      (mmd: MessageAndMetadata[String, String]) => (mmd.key(), mmd.message()))


    var offsets = Array.empty[OffsetRange]

    dstream.transform(rdd => {
      offsets = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    }).filter(filterCompleteOrderData).map(getCityAndDate)


    ssc

  }

  /**
    * 过滤出完成视频的数据
    *
    * @param msg
    * @return
    */
  def filterCompleteOrderData(msg: (String, String)): Boolean = {

    val a: Array[String] = msg._2.split("\t")
    if (a.length == 17) {
      val evenType = a(15)
      "completeOrder".equals(evenType)
    } else {
      false
    }
  }

  /**
    * 数据转换，返回((2019-04-03,北京),1)格式的数据
    *
    * @param msg
    */

  def getCityAndDate(msg: (String, String)): ((String, String), Int) = {
    val a: Array[String] = msg._2.split("\t")
    val ip: String = a(8)
    val evenTime: Long = a(16).toLong

    /**
      * 获取日期
      */
    val date = new Date(evenTime * 1000)
    val evenDay: String = sdf.format(date)

    /**
      * 获取城市
      */
    var regionName = "未知"
    val info: CityInfo = ipdb.findInfo(ip, "zh")
    if (info != null) {
      regionName = info.getRegionName
    }


    ((evenDay, regionName), 1)
  }


}
