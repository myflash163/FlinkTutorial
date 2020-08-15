package com.atguigu.wc
import org.apache.flink.api.scala._

object WordCount {
  def main(args: Array[String]): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val path: String = "target/classes/hello.txt"
    val inputDataSet: DataSet[String] = env.readTextFile(path)

    val worldCountDataSet: AggregateDataSet[(String, Int)] = inputDataSet.flatMap(_.split(" "))
      .map((_, 1))
      .groupBy(0)
      .sum(1)

     worldCountDataSet.print()
  }
}