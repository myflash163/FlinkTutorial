package com.atguigu.apitest

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.functions.{AssignerWithPeriodicWatermarks, AssignerWithPunctuatedWatermarks, KeyedProcessFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

object WindowTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //env.getConfig.setAutoWatermarkInterval(100)

    //从文件中读取数据
    val path: String = "target/classes/sensor.txt"
    //val stream: DataStream[String] = env.readTextFile(path)

    //nc -lk 7777
    val stream = env.socketTextStream("localhost", 7777)

    //transform
    val dataStream: DataStream[SensorReading] = stream.map(data => {
      val dataArray: Array[String] = data.split(",")
      SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble)
    })
      //      .assignAscendingTimestamps(_.timestamp * 1000)
      //.assignTimestampsAndWatermarks(new MyAssiginer) //设置watermark
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(1)) {
        override def extractTimestamp(element: SensorReading): Long = element.timestamp * 1000
      })
    //统计15s 内的最小温度,隔5s 输出一次
    val minTempPerWindowStream: DataStream[(String, Double)] = dataStream.map(data => (data.id, data.temperature))
      .keyBy(_._1)
      //.window(SlidingEventTimeWindows.of(Time.seconds(15),Time.seconds(5),Time.hours(-8)))
      .timeWindow(Time.seconds(15), Time.seconds(10)) //开时间窗口 processing time
      .reduce((data1, data2) => (data1._1, data1._2.min(data2._2))) //用reduce 做增量聚合

    minTempPerWindowStream.print("min temp")
    dataStream.print("input data")

    env.execute("window test")


  }

}

class MyAssiginer() extends AssignerWithPeriodicWatermarks[SensorReading] {
  val bound = 6000
  var maxTs: Long = Long.MinValue

  override def getCurrentWatermark: Watermark = new Watermark(maxTs - bound)

  override def extractTimestamp(element: SensorReading, previousElementTimestamp: Long): Long = {
    maxTs = maxTs.max(element.timestamp * 1000)
    element.timestamp * 1000
  }
}

class MyAssigner2() extends AssignerWithPunctuatedWatermarks[SensorReading] {
  override def checkAndGetNextWatermark(lastElement: SensorReading, extractedTimestamp: Long): Watermark = new Watermark(extractedTimestamp)

  override def extractTimestamp(element: SensorReading, previousElementTimestamp: Long): Long = {
    element.timestamp * 1000
  }
}

class MyProcess() extends KeyedProcessFunction[String, SensorReading,String]{
  override def processElement(value: SensorReading, ctx: KeyedProcessFunction[String, SensorReading, String]#Context, out: Collector[String]): Unit = {
    ctx.timerService().registerEventTimeTimer(2000)
  }
}