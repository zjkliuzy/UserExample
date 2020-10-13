package com.liu.networkflow

import java.text.SimpleDateFormat

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time


case class ApacheLogEntity(ip:String,userId:String,timestamp:Long,method:String,url:String)
//窗口聚合结果case class
case class PageViewRealCount(url:String,windowEnd:Long,count:Long)
object HotPageNetworkFlow {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val inputStream = env.readTextFile("C:\\Users\\Administrator.SC-202009132113\\IdeaProjects\\UserExample\\NetWorkFlow\\src\\main\\resources\\apache.log")
    val dataSteam = inputStream
      .map(data => {
        val arr = data.split(" ")
        val simpleDateFormat = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
        val ts = simpleDateFormat.parse(arr(3)).getTime
        ApacheLogEntity(arr(0),arr(1),ts,arr(5),arr(6))
      }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[ApacheLogEntity](Time.minutes(1)) {
      override def extractTimestamp(t: ApacheLogEntity): Long = t.timestamp
    })
  }
}
