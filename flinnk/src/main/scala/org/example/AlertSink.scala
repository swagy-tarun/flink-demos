package org.example

import org.apache.flink.annotation.PublicEvolving
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.example.Alert
import org.slf4j.LoggerFactory


/**
 * A sink for outputting alerts.
 */
@PublicEvolving
@SuppressWarnings(Array("unused"))
@SerialVersionUID(1L)
object AlertSink {
  private val LOG = LoggerFactory.getLogger(classOf[AlertSink])
}

@PublicEvolving
@SuppressWarnings(Array("unused"))
@SerialVersionUID(1L)
class AlertSink extends SinkFunction[Alert] {
  override def invoke(value: Alert, context: SinkFunction.Context[_]): Unit = {
    AlertSink.LOG.info(value.toString)
  }
}