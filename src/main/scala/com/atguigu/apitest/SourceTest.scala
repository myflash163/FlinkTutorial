package com.atguigu.apitest

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011

import scala.collection.immutable
import scala.util.Random

case class SensorReading(id: String, timestamp: Long, temperature: Double)

object SourceTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //从自定义的集合中读取数据
    val stream1: DataStream[SensorReading] = env.fromCollection(List(
      SensorReading("sensor_1", 1547718199, 35.80018327300259),
      SensorReading("sensor_6", 1547718201, 15.402984393403084),
      SensorReading("sensor_7", 1547718202, 6.720945201171228),
      SensorReading("sensor_10", 1547718205, 38.101067604893444)
    ))
    stream1.print("stream1").setParallelism(6)

    env.fromElements(1, 2.0, "string").print()

    //从文件中读取数据
    val path: String = "target/classes/sensor.txt"
    val stream2: DataStream[String] = env.readTextFile(path)
    stream2.print("stream2").setParallelism(1)

    //从kafka中读取数据
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")
    properties.setProperty("group.id", "consumer-group")
    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset", "latest")
    val stream3: DataStream[String] = env.addSource(new FlinkKafkaConsumer011[String]("sensor", new SimpleStringSchema(), properties))

    //进入docker中  /opt/kafka_2.12-2.5.0 # ./bin/kafka-console-producer.sh  --broker-list localhost:9092 --topic sensor
    stream3.print("kafka").setParallelism(1)

    //自定义source
   // val stream4: DataStream[SensorReading] = env.addSource(new SensorSource())
    //stream4.print("stream4").setParallelism(2)

    env.execute("source test")
  }

}

class SensorSource() extends SourceFunction[SensorReading] {
  //定义一个flag,表示数据源是否正常运行
  var running: Boolean = true

  //正常生成数据
  override def run(ctx: SourceFunction.SourceContext[SensorReading]): Unit = {
    //初始化一个随机数发生器
    val rand = new Random(20)
    //初始化定义一组传感器温度数据
    var curTemp: immutable.IndexedSeq[(String, Double)] = 1.to(10).map(
      i => ("sensor_" + i, 60 + rand.nextGaussian() * 20)
    )
    //无限循环，产生数据流
    while (running) {
      //在上一次温度的基础上更新温度值
      curTemp = curTemp.map(t => (t._1, t._2 + rand.nextGaussian()))
      val curTime: Long = System.currentTimeMillis()
      curTemp.foreach(t => ctx.collect(SensorReading(t._1, curTime, t._2)))
      Thread.sleep(2000)
    }
  }

  override def cancel(): Unit = {
    running = false
  }
}
