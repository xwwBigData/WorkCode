package com.atguigu.gmall1111.dw.realtime.handler

import java.text.SimpleDateFormat
import java.util.{Date, Set}

import com.alibaba.fastjson.JSON
import com.atguigu.gmall1111.common.constant.GmallConstant
import com.atguigu.gmall1111.common.util.MyEsUtil
import com.atguigu.gmall1111.dw.realtime.bean.StartUpLog
import com.atguigu.gmall1111.dw.realtime.util.{MyKafkaUtil, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import redis.clients.jedis.Jedis

object RealtimeStartupApp {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RealtimeStartupApp")
    val sparkContext = new SparkContext(sparkConf)
    val streamingContext = new StreamingContext(sparkContext,Seconds(5))
      //读取kafka的数据，
    val startupStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstant.KAFKA_TOPIC_STARTUP,streamingContext)

    val startUpLog: DStream[StartUpLog] = startupStream.map(record => {
      //向kafka读取到的数据里面添加时间
      val jeson: String = record.value()
      val startUpLog: StartUpLog = JSON.parseObject(jeson, classOf[StartUpLog])
      val dateTimeString: String = new SimpleDateFormat("yyyy-MM-dd HH:mm").format(new Date(startUpLog.ts))
      val timeArray: Array[String] = dateTimeString.split(" ")
      startUpLog.logDate = timeArray(0)
      val HHmmString: Array[String] = timeArray(1).split(":")
      startUpLog.logHour = HHmmString(0)
      startUpLog.logHourMinute = HHmmString(1)

      startUpLog
    })
    //拿到redis里面的mid list，和数据进行比较，剔除数据流中已经存在mid，形成过滤后的数据流
    val filteredDstream: DStream[StartUpLog] = startUpLog.transform { rdd => {
      val jedis = RedisUtil.getJedisClient
      val curDate: String = new SimpleDateFormat("yyyy-MM-dd").format(new Date())
      val key = "dau:" + curDate
      val dauSet: Set[String] = jedis.smembers(key)
      val dauBc: Broadcast[Set[String]] = streamingContext.sparkContext.broadcast(dauSet)
      jedis.close()
      rdd.filter(startUplog => {
        !dauBc.value.contains(startUplog.mid)
      })
    }
    }

    //考虑到 新的访问可能会出现重复 ，所以以mid为key进行去重，每个mid为小组 每组取其中一个
    val filterMapDstring: DStream[(String, StartUpLog)] = filteredDstream.map(sul => {
      (sul.mid, sul)
    })
    val startuplogFilterDistinctDstream: DStream[StartUpLog] = filterMapDstring.groupByKey().flatMap {
      case (mid, startuplogItr) => {
        //注意，下面只取了一个，为什么返回的数据是个集合，因为每个mid 只取一个，但是有多个mid ,所以是一个集合
        val startupLogOnceItr: Iterable[StartUpLog] = startuplogItr.take(1)
        startupLogOnceItr
      }
    }

    startuplogFilterDistinctDstream.foreachRDD(rdd=>{
      //用foreachPartition 为了性能，可以少建立一点redis连接
      rdd.foreachPartition(startupLogItr=>{
        val jedis: Jedis = RedisUtil.getJedisClient
         val startupLogList: List[StartUpLog] = startupLogItr.toList

        for (startupLog <- startupLogList) {
          val key = "dau:" + startupLog.logDate
          jedis.sadd(key,startupLog.mid)
        }
        jedis.close()
        //数据放到es
        MyEsUtil.insertEsBulk(GmallConstant.ES_INDEX_DAU, startupLogList)
      })
      
    })
    streamingContext.start()
    streamingContext.awaitTermination()
  }

}
