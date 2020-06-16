package org.example

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}


class MonitorWorkTime extends KeyedProcessFunction[String, TaxiRide, (String, String)] {

  private val ALLOWED_WORK_TIME = 12 * 60 * 60 * 1000 // 12 hours

  private val REQ_BREAK_TIME = 8 * 60 * 60 * 1000 // 8 hours

  private val CLEAN_UP_INTERVAL = 24 * 60 * 60 * 1000 // 24 hours

  @transient private var formatter: DateTimeFormatter = _
  var shiftStart: ValueState[java.lang.Long] = _

  override def open(parameters: Configuration): Unit = {
    shiftStart = getRuntimeContext.getState(new ValueStateDescriptor[java.lang.Long]("shiftStart", Types.LONG))
    formatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss")
  }

  override def processElement(value: TaxiRide, ctx: KeyedProcessFunction[String, TaxiRide, (String, String)]#Context,
                              out: Collector[(String, String)]): Unit = {
    var startTs = shiftStart.value()

    if (startTs == null || startTs < value.pickUpTime - (ALLOWED_WORK_TIME + REQ_BREAK_TIME)) {
      startTs = value.pickUpTime
      shiftStart.update(startTs)
      val endTs = startTs + ALLOWED_WORK_TIME
      out.collect(Tuple2.apply(value.licenseId, "New shift started. Shifts end at "
        + formatter.print(endTs) + "."))
      ctx.timerService().registerEventTimeTimer(startTs + CLEAN_UP_INTERVAL)

    } else if (startTs < value.pickUpTime - ALLOWED_WORK_TIME) {
      out.collect(Tuple2.apply(value.licenseId, "This ride violated the working time regulations."))
    }
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, TaxiRide, (String, String)]#OnTimerContext, out: Collector[(String, String)]): Unit = {
    val startTs = shiftStart.value()
    if (startTs == timestamp - CLEAN_UP_INTERVAL) {
      shiftStart.clear()
    }
  }
}
