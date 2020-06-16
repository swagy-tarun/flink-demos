package org.example.jobs

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.example.{MonitorWorkTime, TaxiRide, TaxiRides}
import org.apache.flink.streaming.api.scala._

object TaxiWorkingTimeMonitor {

  def main(args: Array[String]): Unit = {
    if (args.length != 1) {
      System.err.println("Please provide the path for taxi rides file as parameter")
    }

    val input: String = args(0)

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.getConfig.setAutoWatermarkInterval(1000L)

    val rides: DataStream[TaxiRide] = TaxiRides.getRides(env, input)

    val notifications: DataStream[(String, String)]
    = rides.keyBy(r => r.licenseId).process(new MonitorWorkTime).name("MonitorWorkTime")

    notifications.print()

    env.execute()
  }
}
