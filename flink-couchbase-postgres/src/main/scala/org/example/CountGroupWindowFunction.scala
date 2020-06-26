package org.example

import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import org.slf4j.LoggerFactory

class CountGroupWindowFunction extends ProcessWindowFunction[BreweryResult, BreweryResult, String, TimeWindow] {
  private val LOG = LoggerFactory.getLogger(classOf[CountGroupFunctionWithEventTimeProcessing])

  override def process(key: String, context: Context, elements: Iterable[BreweryResult], out: Collector[BreweryResult]): Unit = {
    val cdrData = elements.iterator.next()
    val result = BreweryResult(cdrData.accountId, cdrData.count, context.window.getStart, context.window.getEnd, cdrData.extra)
    LOG.info("From Window function: {}", result)
    out.collect(result)
  }
}
