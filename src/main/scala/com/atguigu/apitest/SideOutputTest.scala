package com.atguigu.apitest

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

object SideOutputTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //nc -lk 7777
    val stream = env.socketTextStream("localhost", 7777)

    //transform
    val dataStream: DataStream[SensorReading] = stream.map(data => {
      val dataArray: Array[String] = data.split(",")
      SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble)
    }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(1)) {
      override def extractTimestamp(element: SensorReading): Long = element.timestamp * 1000
    })

    val processedStream: DataStream[SensorReading] = dataStream
      .process(new FreezingAlert())

    //dataStream.print("input data")
    processedStream.print("processed data")
    processedStream.getSideOutput(new OutputTag[String]("freezing alert")).print("alert data")

    env.execute("window test")
  }

}

//冰点报警，如果小于32F,输出报警信息到侧输出流
class FreezingAlert() extends ProcessFunction[SensorReading, SensorReading] {
  private lazy val alertOutput = new OutputTag[String]("freezing alert")

  override def processElement(value: SensorReading, ctx: ProcessFunction[SensorReading, SensorReading]#Context, out: Collector[SensorReading]): Unit = {
    if (value.temperature < 32.0) {
      ctx.output(alertOutput, "freezing alert for " + value.id)
    } else {
      out.collect(value)
    }
  }
}