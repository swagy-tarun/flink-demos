package org.example

/**
 * MIT License
 * <p>
 * Copyright (c) 2020 Tarun Arora
 * <p>
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this software and
 * associated documentation files (the "Software"), to deal in the Software without restriction,
 * including without limitation the rights to use, copy, modify, merge, publish, distribute,
 * sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 * <p>
 * The above copyright notice and this permission notice shall be included in all copies or
 * substantial portions of the Software.
 * <p>
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT
 * NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
 * DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector
import org.slf4j.LoggerFactory

class CountGroupFunction extends KeyedProcessFunction[String, Brewery, (CDRData)] {
  private val LOG = LoggerFactory.getLogger(classOf[CountGroupFunction])
  var state: ValueState[CountWithTimestamp] = _

  override def open(parameters: Configuration): Unit = {
    state = getRuntimeContext.getState(new ValueStateDescriptor[CountWithTimestamp]("count-brew-state", classOf[CountWithTimestamp]))
    if (state == null) {
      LOG.info("Restored state is null")
      return
    }
  }

  override def processElement(value: Brewery, ctx: KeyedProcessFunction[String, Brewery, (CDRData)]#Context,
                              out: Collector[(CDRData)]): Unit = {
    LOG.info("Id {} of Record Fetched: {}", value.getDocumentId, value.getBreweryId: Any)
    val key = value.getBreweryId
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

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, Brewery, (CDRData)]#OnTimerContext,
                       out: Collector[(CDRData)]): Unit = {
    val cdr = CDRData(state.value().key, state.value().count, state.value().currentProcessingTime, ctx.timerService().currentProcessingTime())
    state.clear()
    LOG.info(cdr.toString)
    out.collect(cdr)
  }
}


