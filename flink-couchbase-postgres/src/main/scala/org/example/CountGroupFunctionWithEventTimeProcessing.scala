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

import org.apache.flink.api.common.functions.AggregateFunction
import org.slf4j.LoggerFactory

class CountGroupFunctionWithEventTimeProcessing extends AggregateFunction[Brewery, CountWithTimestamp, CDRData] {
  private val LOG = LoggerFactory.getLogger(classOf[CountGroupFunctionWithEventTimeProcessing])

  override def createAccumulator(): CountWithTimestamp = {
    CountWithTimestamp("", 0, 5385538, "")
  }

  override def add(value: Brewery, accumulator: CountWithTimestamp): CountWithTimestamp = {
    LOG.info("Id {} of Record Fetched: {} with EventTime: {}", value.getDocumentId, value.getBreweryId, value.getEventTime)
    CountWithTimestamp(value.getBreweryId, accumulator.count + 1, accumulator.currentProcessingTime, accumulator.extra.concat(value.getDocumentId).concat("--"))
  }

  override def getResult(accumulator: CountWithTimestamp): CDRData = {
    val out = CDRData(accumulator.key, accumulator.count, accumulator.currentProcessingTime, accumulator.currentProcessingTime, accumulator.extra)
    //LOG.info(out.toString)
    out
  }

  override def merge(a: CountWithTimestamp, b: CountWithTimestamp): CountWithTimestamp = {
    LOG.info("Merged called")
    CountWithTimestamp(a.key, a.count + b.count, b.currentProcessingTime, b.extra)
  }
}


