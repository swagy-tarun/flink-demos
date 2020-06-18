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
import java.sql.{Connection, DriverManager}

import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.state.{FunctionInitializationContext, FunctionSnapshotContext}
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.slf4j.LoggerFactory

class PostgresSqlSinkFunction extends RichSinkFunction[CDRData] with CheckpointedFunction {

  private val Log = LoggerFactory.getLogger(classOf[PostgresSqlSinkFunction])

  import java.sql.PreparedStatement

  private val UPSERT_CASE = "INSERT INTO public.brewery_counts (brewery_id, count, start_time, end_time, window_hash) " + "VALUES (?, ?, ?, ?, ?) "

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
