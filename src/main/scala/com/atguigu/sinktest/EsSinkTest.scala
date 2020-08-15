package com.atguigu.sinktest

import java.util

import com.atguigu.apitest.SensorReading
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.http.HttpHost
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.Requests

object EsSinkTest {
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
    val httpHosts = new util.ArrayList[HttpHost]()
    httpHosts.add(new HttpHost("localhost", 9200))
    //创建一个esSink 的builder
    val esSinkBuilder = new ElasticsearchSink.Builder[SensorReading](
      httpHosts, new ElasticsearchSinkFunction[SensorReading] {
        override def process(element: SensorReading, runtimeContext: RuntimeContext, requestIndexer: RequestIndexer): Unit = {
          print("saving data:" + element)
          val json = new util.HashMap[String, String]()
          json.put("sensor_id", element.id)
          json.put("temperature", element.temperature.toString)
          json.put("ts", element.timestamp.toString)
          //创建index request 准备发送数据
          val indexRequest: IndexRequest = Requests.indexRequest().index("sensor")
            .`type`("readingdata")
            .source(json)
          //利用index 发送请求，写入数据
          requestIndexer.add(indexRequest)
          println("data saved")
        }
      }
    )

    //sink
    dataStream.addSink(esSinkBuilder.build())

    env.execute("es sink test")

  }

}
