package com.atguigu.sinktest

import com.atguigu.apitest.SensorReading
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

object RedisSinkTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    //从文件中读取数据
    val path: String = "target/classes/sensor.txt"
    val inputStream: DataStream[String] = env.readTextFile(path)
    inputStream.setParallelism(1)

    //transform
    val dataStream: DataStream[SensorReading] = inputStream.map(data => {
      val dataArray: Array[String] = data.split(",")
      SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble)
    })

    val conf: FlinkJedisPoolConfig = new FlinkJedisPoolConfig.Builder().setHost("localhost")
      .setPort(6379).build()
    //sink
    dataStream.addSink(new RedisSink(conf,new MyRedisMapper))
    env.execute("redis sink test")
  }

}

class MyRedisMapper() extends RedisMapper[SensorReading] {
  override def getCommandDescription: RedisCommandDescription = {
    new RedisCommandDescription(RedisCommand.HSET, "sensor_temperature")
  }

  override def getKeyFromData(t: SensorReading): String = {
    t.temperature.toString
  }

  override def getValueFromData(t: SensorReading): String = {
    t.id
  }
}
