package com.atguigu.wc

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._

object StreamWordCount {
  def main(args: Array[String]): Unit = {
    val params: ParameterTool = ParameterTool.fromArgs(args)
    val host: String = params.get("host")
    val port: Int = params.getInt("port")

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //env.setParallelism(1) //设置并行度
    //env.disableOperatorChaining()  //禁止组合任务

    val dataStream: DataStream[String] = env.socketTextStream(host, port)

    val workCountDataStream: DataStream[(String, Int)] = dataStream.flatMap(_.split("\\s"))
      .filter(_.nonEmpty)//.startNewChain()//.disableChaining()
      .map((_, 1))
      .keyBy(0)
      .sum(1)

    workCountDataStream.print().setParallelism(2)

    //启动executor
    env.execute("stream world count job")

    //nc -lk 7777
  }

}
