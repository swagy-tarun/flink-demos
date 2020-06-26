package org.example

import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import org.slf4j.LoggerFactory

class CountGroupWindowFunction extends ProcessWindowFunction[CDRData, CDRData, String, TimeWindow] {
  private val LOG = LoggerFactory.getLogger(classOf[CountGroupFunctionWithEventTimeProcessing])

  override def process(key: String, context: Context, elements: Iterable[CDRData], out: Collector[CDRData]): Unit = {
    val cdrData = elements.iterator.next()
    val result = CDRData(cdrData.accountId, cdrData.count, context.window.getStart, context.window.getEnd, cdrData.extra)
    LOG.info("From Window function: {}", result)
    out.collect(result)
  }
}
