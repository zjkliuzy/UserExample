package com.example

import java.sql.Timestamp
import java.util.Properties

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.java.tuple.{Tuple, Tuple1}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer


//定义输入数据阳历咧
case class UserBehavior(userId: Long, itemId: Long, categoryId: Int, behaivor: String, timestamp: Long)

//定义窗口聚合结果阳历咧
case class ItemViewCount(itemId: Long, windowEnd: Long, count: Long)


object HotItem {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //从文件读取数据
    //val inputStream = env.readTextFile("C:\\Users\\Administrator.SC-202009132113\\IdeaProjects\\UserExample\\HotItemAna\\src\\main\\resources\\UserBehavior.csv")
    //从kafka里面读取数据
    val porperites = new Properties()
    porperites.setProperty("bootstrap.servers", "192.168.31.202:9092")
    porperites.setProperty("group.id", "consumer-group")
    porperites.setProperty("key.deserializer",
      "org.apache.kafka.common.serialization.StringDeserializer")
    porperites.setProperty("value.deserializer",
      "org.apache.kafka.common.serialization.StringDeserializer")
    val inputStream = env.addSource(new FlinkKafkaConsumer[String]("hotitem", new SimpleStringSchema(), porperites))

    val dataStream = inputStream
      .map(data => {
        val arr = data.split(",")
        UserBehavior(arr(0).toLong, arr(1).toLong, arr(2).toInt, arr(3), arr(4).toLong)
      }).assignAscendingTimestamps(_.timestamp * 1000)

    //得到窗口聚合结果
    val aggStream: DataStream[ItemViewCount] = dataStream
      .filter(_.behaivor == "pv")
      .keyBy("itemId")
      .timeWindow(Time.hours(1), Time.minutes(5))
      .aggregate(new CountAgg(), new ItemWidowsResult())

    val resultStream = aggStream
      .keyBy("windowEnd")
      .process(new TopItem(5)) //自定义处理流程

    resultStream.print("reult")


    env.execute("hotitem")
  }

}

class CountAgg() extends AggregateFunction[UserBehavior, Long, Long] {
  override def createAccumulator(): Long = 0L

  override def add(value: UserBehavior, accumulator: Long): Long = accumulator + 1 //每来一条数据调用add（）

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a + b
}

//自定义窗口函数
class ItemWidowsResult extends WindowFunction[Long, ItemViewCount, Tuple, TimeWindow] {

  override def apply(key: Tuple, window: TimeWindow, input: Iterable[Long], out: Collector[ItemViewCount]): Unit = {
    val itemId = key.asInstanceOf[Tuple1[Long]].f0
    val windowEnd = window.getEnd
    val count = input.iterator.next()
    out.collect(ItemViewCount(itemId, windowEnd, count))

  }
}

class TopItem(size: Int) extends KeyedProcessFunction[Tuple, ItemViewCount, String] {
  //定义状态
  var itemViewCountListState: ListState[ItemViewCount] = _

  override def open(parameters: Configuration): Unit = {
    itemViewCountListState = getRuntimeContext.getListState(new ListStateDescriptor[ItemViewCount]("itemViewCount-list", classOf[ItemViewCount]))
  }

  override def processElement(i: ItemViewCount, context: KeyedProcessFunction[Tuple, ItemViewCount, String]#Context, collector: Collector[String]): Unit = {
    //每来一条数据加入itemViewCountListState
    itemViewCountListState.add(i)
    //注册一个windowEnd+1后的定时器
    context.timerService().registerEventTimeTimer(i.windowEnd + 1)
  }

  //当定时器触发，认为所有结果到齐
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Tuple, ItemViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {

    //定义一个ListBuffer，保存liststaet里面数据
    val allItemViewCounts: ListBuffer[ItemViewCount] = ListBuffer()
    val iter = itemViewCountListState.get().iterator()
    while (iter.hasNext) {
      allItemViewCounts += iter.next()
    }
    //清空状态
    itemViewCountListState.clear()
    //排序
    val sortedList = allItemViewCounts.sortBy(_.count)(Ordering.Long.reverse).take(size)

    val resultString = new StringBuilder
    resultString.append("窗口结束时间：").append(new Timestamp(timestamp - 1)).append("\n")
    //遍历结果列表中的每个ItemViewCount
    for (i <- sortedList.indices) {
      val currentItemCount = sortedList(i)
      resultString.append("NO").append(i + 1).append(": ")
        .append("商品= ").append(currentItemCount.itemId).append("\t")
        .append("热门度=").append(currentItemCount.count).append("\n")

    }
    resultString.append("=========\n\n")
    Thread.sleep(1000)
    out.collect(resultString.toString())
  }
}
