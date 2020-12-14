package com.ychenchen.wordcount

import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, createTypeInformation}

object WordCount1 {
  def main(args: Array[String]): Unit = {
    val environment = StreamExecutionEnvironment.getExecutionEnvironment

    val data = environment.socketTextStream("localhost", 9999)

    val value = data.flatMap(line => line.split(",")).map((_, 1)).keyBy(0).sum(1)

    value.print()

    environment.execute("wordCount")

  }
}
