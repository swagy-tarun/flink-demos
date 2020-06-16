package org.example

import com.couchbase.client.java.json.JsonObject
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector
import org.slf4j.LoggerFactory

class CountGroupFunction extends KeyedProcessFunction[String, JsonObject, (CDRData)] {
  private val LOG = LoggerFactory.getLogger(classOf[CountGroupFunction])
  var state: ValueState[CountWithTimestamp] = _

  override def open(parameters: Configuration): Unit = {
    state = getRuntimeContext.getState(new ValueStateDescriptor[CountWithTimestamp]("count-brew-state", classOf[CountWithTimestamp]))
    if (state == null) {
      LOG.info("Restored state is null")
      return
    }
  }

  override def processElement(value: JsonObject, ctx: KeyedProcessFunction[String, JsonObject, (CDRData)]#Context,
                              out: Collector[(CDRData)]): Unit = {
    // initialize or retrieve/update the state
    //LOG.info("State before calling: {}", state.value(
    LOG.info("Id {} of Record Fetched: {}", value.get("document_id"), value.get("brewery_id"): Any)
    val key = String.valueOf(value.get("brewery_id"))
    val currentProcessingTime = ctx.timerService().currentProcessingTime()
    val current: CountWithTimestamp = state.value match {
      case null =>
        CountWithTimestamp(key, 1, currentProcessingTime)
      case CountWithTimestamp(key, count, lastModified) =>
        if (currentProcessingTime > lastModified + 10000) {
          ctx.timerService.registerProcessingTimeTimer(currentProcessingTime)
          CountWithTimestamp(key, count + 1, currentProcessingTime)
        } else {
          CountWithTimestamp(key, count + 1, lastModified)
        }
    }

    state.update(current)
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, JsonObject, (CDRData)]#OnTimerContext,
                       out: Collector[(CDRData)]): Unit = {
    val cdr = CDRData(state.value().key, state.value().count, state.value().currentProcessingTime, ctx.timerService().currentProcessingTime())
    state.clear()
    LOG.info(cdr.toString)
    out.collect(cdr)
  }
}


