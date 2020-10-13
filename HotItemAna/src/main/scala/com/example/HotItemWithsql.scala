package com.example

import org.apache.flink.types._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.{EnvironmentSettings, Slide}
import org.apache.flink.table.api.scala._

object HotItemWithsql {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val inputStream = env.readTextFile("C:\\Users\\Administrator.SC-202009132113\\IdeaProjects\\UserExample\\HotItemAna\\src\\main\\resources\\UserBehavior.csv")
    val dataStream = inputStream
      .map(data => {
        val arr = data.split(",")
        UserBehavior(arr(0).toLong, arr(1).toLong, arr(2).toInt, arr(3), arr(4).toLong)
      }).assignAscendingTimestamps(_.timestamp * 1000)

    val settings = EnvironmentSettings
      .newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build()
    val tableEnv = StreamTableEnvironment.create(env, settings)

    val dataTable = tableEnv.fromDataStream(dataStream,
      'itemId,
      'behaivor,
      'timestamp.rowtime as 'ts)

    val aggtable = dataTable
      .filter('behaivor === "pv")
      .window(Slide over 1.hours every 5.minutes on 'ts as 'sw)
      .groupBy('itemId, 'sw)
      .select('itemId, 'sw.end as 'winEnd, 'itemId.count as 'cn)
    tableEnv.createTemporaryView("aggTable", aggtable, 'itemId, 'winEnd, 'cn)
    val resultTable = tableEnv.sqlQuery(
      """
        |select * from
        | (select *,row_number()
        |   over (partition by winEnd order by cn desc)
        |   as row_num
        |   from aggTable)
        |where row_num <=5
        |
        |""".stripMargin
    )
    //纯sql实现
    tableEnv.createTemporaryView("dataTable", dataStream, 'itemId, 'behaivor,
      'timestamp.rowtime as 'ts)
    val resultSql = tableEnv.sqlQuery(
      """
       select * from
        | (select *,row_number()
        |   over (partition by winEnd order by cn desc)
        |   as row_num
        |   from (select
        |       itemId,
        |       hop_end(ts,interval '5' minute ,interval '1' hour) as winEnd,
        |       count(itemId) as cn
        |
        |       from dataTable
        |       where behaivor ='pv'
        |       group by
        |       itemId,
        |       hop(ts,interval '5' minute ,interval '1' hour)
        |   )
        |  )
        |where row_num <=5
        |
        |
        |
        |""".stripMargin
    )

    resultTable.toRetractStream[Row].print()
    env.execute("hotsql")
  }
}
