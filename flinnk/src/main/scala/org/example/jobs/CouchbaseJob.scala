package org.example.jobs

import java.nio.file.Paths
import java.time.ZoneId

import org.apache.flink.api.common.time
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.example._
import org.slf4j.LoggerFactory

object CouchbaseJob {
  private val LOG = LoggerFactory.getLogger(CouchbaseJob.getClass)

  @throws[Exception]
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //env.registerType(CountGroupFun)
    //env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    // env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, time.Time.of(10, TimeUnit.SECONDS)))
    env.enableCheckpointing(30000)
    env.getCheckpointConfig.setPreferCheckpointForRecovery(true)
    // required to support recovery from checkpoint
    env.getCheckpointConfig.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
    env.setStateBackend(new FsStateBackend(Paths.get("D:\\flink-checkpoints").toUri, false))
    // required to overcome serialization exceptions
    env.registerType(classOf[CountWithTimestamp])

    val queryInput = new CouchbaseSourceQuery("test-inserts", "updated", classOf[Brewery],
      "yyyy-MM-dd HH:mm:ss", ZoneId.of("Asia/Calcutta"))

    val transactions: DataStream[Brewery] = env
      .addSource(new CouchbaseVersion3Source[Brewery](time.Time.seconds(5), queryInput)).name("couchbase-source")


    /*val counts = transactions.map { row =>
      // println(row.get("brewery_id"))
      (String.valueOf(row.get("brewery_id")), 1)
    }.keyBy(0)
      .timeWindow(Time.seconds(10))
      .sum(1)*/

    val counts = transactions.keyBy(row => row.getBreweryId())
      .process(new CountGroupFunction)
    /*transactions.addSink(new SinkFunction[JsonObject] {

      override def invoke(value: JsonObject, context: SinkFunction.Context[_]): Unit = {
        LOG.info(value.toString)
      }
    })*/
    // counts.print()
    counts.addSink(new PostgresSqlSinkFunction).name("postgres-sink")

    env.execute("couchbase-job")
  }
}
