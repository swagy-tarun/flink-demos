package org.example

import java.time.format.DateTimeFormatter
import java.time.{LocalDateTime, ZoneId, ZonedDateTime}
import java.{lang, util}
import java.util.Collections

import org.apache.flink.streaming.api.checkpoint.ListCheckpointed
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.watermark.Watermark
import org.slf4j.LoggerFactory

class TimestampExtractorAndWatermarkEmitter(zoneId: ZoneId, formatter: String) extends AssignerWithPeriodicWatermarks[Brewery] with ListCheckpointed[lang.Long] {
  private val Log = LoggerFactory.getLogger(classOf[TimestampExtractorAndWatermarkEmitter])
  val maxOutOfOrderness = 60000
  var currentMaxTimeStamp: Long = System.currentTimeMillis()

  override def getCurrentWatermark: Watermark = {
    val watermark = new Watermark(currentMaxTimeStamp - maxOutOfOrderness)
    //Log.info("Current Watermark: {}", watermark.getTimestamp)
    watermark
  }

  override def extractTimestamp(element: Brewery, previousElementTimestamp: Long): Long = {
    val dtFormatter = DateTimeFormatter.ofPattern(formatter)
    val eventTimeString = element.getEventTime
    val eventLocalTime = LocalDateTime.parse(eventTimeString, dtFormatter)
    val eventTimeInLong = ZonedDateTime.of(eventLocalTime, zoneId).toEpochSecond * 1000
    currentMaxTimeStamp = Math.max(eventTimeInLong, currentMaxTimeStamp)
    eventTimeInLong
  }

  override def snapshotState(checkpointId: Long, timestamp: Long): util.List[lang.Long] = {
    Collections.singletonList(currentMaxTimeStamp.longValue())
  }

  override def restoreState(state: util.List[lang.Long]): Unit = {
    if (!state.isEmpty) this.currentMaxTimeStamp = state.get(0)
    else Log.warn("State is empty")
  }
}
