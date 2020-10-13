package com.liu.networkflow

import java.text.SimpleDateFormat

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector


case class ApacheLogEntity(ip: String, userId: String, timestamp: Long, method: String, url: String)

//窗口聚合结果case class
case class PageViewRealCount(url: String, windowEnd: Long, count: Long)

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
        ApacheLogEntity(arr(0), arr(1), ts, arr(5), arr(6))
      }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[ApacheLogEntity](Time.minutes(1)) {
      override def extractTimestamp(t: ApacheLogEntity): Long = t.timestamp
    })

    val aggStream = dataSteam
      .filter(_.method == "GET")
      .keyBy(_.url)
      .timeWindow(Time.minutes(10), Time.seconds(5))
      .aggregate(new PageCountagg())
  }
}

class PageCountagg() extends AggregateFunction[ApacheLogEntity, Long, Long] {
  override def createAccumulator(): Long = 0L

  override def add(value: ApacheLogEntity, accumulator: Long): Long = accumulator + 1

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a + b
}

class PageWindowFunc() extends WindowFunction[Long, PageViewRealCount, String, TimeWindow] {
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[PageViewRealCount]): Unit = {
    out.collect(PageViewRealCount(key, window.getEnd, input.iterator.next()))
  }
}

