package org.example.jobs

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

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.time.Time
import org.example._
import org.slf4j.LoggerFactory

import java.nio.file.Paths
import java.time.{Duration, ZoneId}

object CouchbaseJobKappaPlus {
  private val LOG = LoggerFactory.getLogger(CouchbaseJob.getClass)

  @throws[Exception]
  def main(args: Array[String]): Unit = {

    val parametersFile = "D:\\tools\\flink-1.10.0\\conf\\couchjob.conf"
    val parameterTool: ParameterTool = ParameterTool.fromPropertiesFile(parametersFile)
    // field to run the job X hours or minutes behind
    val retro = Integer.valueOf(parameterTool.getRequired("couchkappa.retro")).longValue()
    LOG.info("Value for Retro: {}", retro)
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.getConfig.getGlobalJobParameters
    //env.registerType(CountGroupFun)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    // env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, time.Time.of(10, TimeUnit.SECONDS)))
    env.enableCheckpointing(60000)
    env.getCheckpointConfig.setPreferCheckpointForRecovery(true)
    // required to support recovery from checkpoint
    env.getCheckpointConfig.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
    env.setStateBackend(new FsStateBackend(Paths.get("D:\\flink-checkpoints").toUri, false))
    // required to overcome serialization exceptions
    env.registerType(classOf[CountWithTimestamp])
    env.setParallelism(1)

    val zoneId = ZoneId.of("Asia/Calcutta")
    val dateFormat = "yyyy-MM-dd HH:mm:ss"
    val eventTimeDf = dateFormat + ":SSS"
    // To run a data correction job as per Kappa architecture we need to query data by eventTime rather than Updated Time
    // based on the assumption that all data would have arrived in the source by this time.
    val queryInput = new CouchbaseSourceQuery(Duration.ofSeconds(5), "test-inserts", "eventTime", classOf[Brewery],
      dateFormat, zoneId)
    // Fast forward should be disabled for data correction job since it should always run X amount of time behind main job
    val queryCatchupConfig = QueryCatchupConfig.build(Duration.ofSeconds(retro), false,
      Duration.ofSeconds(60), Duration.ofSeconds(120))

    // Source Config
    val transactions: DataStream[Brewery] = env
      .addSource(new CouchbaseVersion3Source[Brewery](new CouchbaseClusterInfo("localhost",
        "admin", "password"), queryInput,
        queryCatchupConfig)).name("couchbase-source")
      .assignTimestampsAndWatermarks(
        new TimestampExtractorAndWatermarkEmitter(zoneId, eventTimeDf, 100,
          System.currentTimeMillis() - (retro * 1000)))

    val counts = transactions.keyBy(row => row.getBreweryId()).timeWindow(Time.seconds(10))
      .aggregate(new CountGroupFunctionWithEventTimeProcessing, new CountGroupWindowFunction)

    counts.addSink(new IdempotentPostgresSqlSinkFunction).name("postgres-sink")

    env.execute("couchbase-kappa-job")
  }
}
