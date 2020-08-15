package com.atguigu.apitest

import org.apache.flink.api.common.functions.{FilterFunction, RichMapFunction}
import org.apache.flink.streaming.api.scala._

object TranformTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    //1、基本转换算子和简单聚合算子
    val path: String = "target/classes/sensor.txt"
    val stream: DataStream[String] = env.readTextFile(path)
    val dataStream: DataStream[SensorReading] = stream.map(data => {
      val dataArray: Array[String] = data.split(",")
      SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble)
    })
      //.keyBy(0).sum(2)
      //.keyBy("id").sum("temperature")

      //输出当前传感器最新的温度+10，而时间戳是上一次数据的时间+1
      .keyBy("id").reduce((x, y) => SensorReading(x.id, x.timestamp + 1, y.temperature + 10))
    dataStream.print()

    //2、多流转换算子
    val splitStream: SplitStream[SensorReading] = dataStream.split(data => {
      if (data.temperature > 30) Seq("high") else Seq("low")
    })
    val high: DataStream[SensorReading] = splitStream.select("high")
    val low: DataStream[SensorReading] = splitStream.select("low")
    val all: DataStream[SensorReading] = splitStream.select("high", "low")

    high.print("high")
    low.print("low")
    all.print("all")
    //3、合并两条流 只能合并两条流 可以数据类型不一致
    val waring: DataStream[(String, Double)] = high.map(data => (data.id, data.temperature))
    val connectedStream: ConnectedStreams[(String, Double), SensorReading] = waring.connect(low)
    val coMapDataStream: DataStream[Product] = connectedStream.map(
      waringData => (waringData._1, waringData._2, "waring"),
      lowData => (lowData.id, "healthy")
    )
    coMapDataStream.print()
    //union 数据类型必须一直
    val unionStream: DataStream[SensorReading] = high.union(low)
    unionStream.print("union")

    //函数类
    dataStream.filter(new MyFilter).print()


    env.execute("transform test")
  }

}

class MyFilter() extends FilterFunction[SensorReading] {
  override def filter(value: SensorReading): Boolean = {
    value.id.startsWith("sensor_1")

  }
}
class MyMapper() extends RichMapFunction[SensorReading, String]{
  override def map(value: SensorReading): String = {
    "flink"
  }
}