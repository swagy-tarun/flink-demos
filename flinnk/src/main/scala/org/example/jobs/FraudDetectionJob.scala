package org.example.jobs

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.example._
import org.apache.flink.streaming.api.scala._

object FraudDetectionJob {

  @throws[Exception]
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val transactions: DataStream[Transaction] = env
      .addSource(new TransactionSource())
      .name("transactions")

    val alerts: DataStream[Alert] = transactions
      .keyBy(transaction => transaction.getAccountId)
      .process(new FraudDetector)
      .name("fraud-detector")

    alerts
      .addSink(new AlertSink).name("alert-sink")

    env.execute("Fraud Detection")
  }

}
