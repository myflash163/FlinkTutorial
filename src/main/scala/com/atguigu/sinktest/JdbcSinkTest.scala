package com.atguigu.sinktest


import java.sql.{Connection, DriverManager, PreparedStatement}

import com.atguigu.apitest.SensorReading
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala._

object JdbcSinkTest {
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
    //sink
    dataStream.addSink(new MyJdbcSink())

    env.execute("jdbc sink test")

  }

}

class MyJdbcSink() extends RichSinkFunction[SensorReading] {
  //定义sql连接，预编译器
  var conn: Connection = _
  var insertStmt: PreparedStatement = _
  var updateStmt: PreparedStatement = _

  //初始化，创建连接和预编译语句
  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/test", "root", "123456789")
    insertStmt = conn.prepareStatement("INSERT into temperatures(sensor,temp) VALUES (?,?)")
    updateStmt = conn.prepareStatement("UPDATE temperatures SET temp = ? where sensor = ?")
  }

  //调用连接，执行sql
  override def invoke(value: SensorReading, context: SinkFunction.Context[_]): Unit = {
    updateStmt.setDouble(1, value.temperature)
    updateStmt.setString(2, value.id)
    updateStmt.execute()
    if (updateStmt.getUpdateCount == 0) {
      insertStmt.setString(1, value.id)
      insertStmt.setDouble(2, value.temperature)
      insertStmt.execute()
    }
  }

  //关闭时做清理工作
  override def close(): Unit = {
    insertStmt.close()
    updateStmt.close()
    conn.close()

  }
}