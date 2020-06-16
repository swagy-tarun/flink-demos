package org.example

import java.sql.{Connection, DriverManager}

import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.state.{FunctionInitializationContext, FunctionSnapshotContext}
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.slf4j.LoggerFactory

class PostgresSqlSinkFunction extends RichSinkFunction[CDRData] with CheckpointedFunction {

  private val Log = LoggerFactory.getLogger(classOf[PostgresSqlSinkFunction])

  import java.sql.PreparedStatement

  private val UPSERT_CASE = "INSERT INTO public.account_counts (account_id, count, start_time, end_time, window_hash) " + "VALUES (?, ?, ?, ?, ?) "

  private var statement: PreparedStatement = _
  private var conn: Connection = _

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)

    val url = "jdbc:postgresql://localhost/flink?user=postgres&password=admin&ssl=false"
    Class.forName("org.postgresql.Driver")
    conn = DriverManager.getConnection(url)
    conn.setAutoCommit(false)
    statement = conn.prepareStatement(UPSERT_CASE)
  }

  override def invoke(value: CDRData, context: SinkFunction.Context[_]): Unit = {

    statement.setString(1, value.accountId)
    statement.setInt(2, value.count)
    statement.setLong(3, value.start)
    statement.setLong(4, value.end)
    statement.setLong(5, value.windowHash)
    statement.addBatch()
  }


  override def close(): Unit = {
    if (statement != null) statement.close()
    if (conn != null) conn.close()
    super.close()
  }

  override def snapshotState(context: FunctionSnapshotContext): Unit = {
    val result = statement.executeBatch()
    conn.commit()
    Log.info("Executed sink for number of records: {}", result.size)
  }

  override def initializeState(context: FunctionInitializationContext): Unit = {

  }
}
