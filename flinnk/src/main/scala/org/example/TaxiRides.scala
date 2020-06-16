package org.example

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.typeinfo.{TypeInformation, Types}
import org.apache.flink.api.java.io.RowCsvInputFormat
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.types.Row
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}
import org.apache.flink.streaming.api.scala._

/**
 * Utility class to ingest a stream of taxi ride events.
 */
object TaxiRides { // field types of the CSV file
  private val inputFieldTypes = Array[TypeInformation[_]](Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.INT, Types.FLOAT, Types.FLOAT, Types.FLOAT, Types.FLOAT, Types.FLOAT, Types.STRING, Types.FLOAT, Types.FLOAT, Types.FLOAT, Types.FLOAT, Types.FLOAT, Types.FLOAT)

  /**
   * Returns a DataStream of TaxiRide events from a CSV file.
   *
   * @param env     The execution environment.
   * @param csvFile The path of the CSV file to read.
   * @return A DataStream of TaxiRide events.
   */
  def getRides(env: StreamExecutionEnvironment, csvFile: String): DataStream[TaxiRide] = { // create input format to read the CSV file
    val inputFormat = new RowCsvInputFormat(null, // input path is configured later
      inputFieldTypes, "\n", ",")
    // read file sequentially (with a parallelism of 1)

    val parsedRows: DataStream[Row] = env.readFile(inputFormat, csvFile).setParallelism(1)
    // convert parsed CSV rows into TaxiRides, extract timestamps, and assign watermarks
    parsedRows.map(new TaxiRides.RideMapper).assignTimestampsAndWatermarks( // define drop-off time as event-time timestamps and generate ascending watermarks.
      new AscendingTimestampExtractor[TaxiRide]() {
        override def extractAscendingTimestamp(ride: TaxiRide): Long = ride.dropOffTime
      }
    )
  }

  /**
   * MapFunction to generate TaxiRide POJOs from parsed CSV data.
   */
  class RideMapper extends RichMapFunction[Row, TaxiRide] {
    private var formatter: DateTimeFormatter = _

    @throws[Exception]
    override def open(parameters: Configuration): Unit = {
      super.open(parameters)
      formatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss")
    }

    @throws[Exception]
    override def map(row: Row): TaxiRide = { // convert time strings into timestamps (longs)
      val pickupTime = formatter.parseDateTime(row.getField(2).asInstanceOf[String]).getMillis
      val dropoffTime = formatter.parseDateTime(row.getField(3).asInstanceOf[String]).getMillis
      // create POJO and set all fields
      val ride = new TaxiRide
      ride.medallion = row.getField(0).asInstanceOf[String]
      ride.licenseId = row.getField(1).asInstanceOf[String]
      ride.pickUpTime = pickupTime
      ride.dropOffTime = dropoffTime
      ride.pickUpLon = row.getField(6).asInstanceOf[Float]
      ride.pickUpLat = row.getField(7).asInstanceOf[Float]
      ride.dropOffLon = row.getField(8).asInstanceOf[Float]
      ride.dropOffLat = row.getField(9).asInstanceOf[Float]
      ride.total = row.getField(16).asInstanceOf[Float]
      ride
    }
  }

}
