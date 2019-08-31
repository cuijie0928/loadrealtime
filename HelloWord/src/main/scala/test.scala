/**
  * Created by cuixj on 2019/8/1.
  */
package com.zctt.dims.loadreal

import org.apache.spark.sql.{CarbonEnv, SparkSession}
import org.apache.spark.sql.CarbonSession._
import org.apache.spark.sql.streaming.{ProcessingTime, StreamingQuery}
import org.apache.carbondata.core.util.path.CarbonTablePath
import org.apache.carbondata.streaming.parser.CarbonStreamParser

object Test {

  def main(args: Array[String]): Unit = {

    val warehouse = args(2)
    val metastore = warehouse

    val spark = SparkSession
      .builder()
      .appName("StreamExample")
      .config("spark.sql.warehouse.dir", warehouse)
      .getOrCreateCarbonSession(warehouse, metastore)

    spark.sparkContext.setLogLevel("ERROR")

    val carbonTable = CarbonEnv.getCarbonTable(Some("default"), args(3))(spark)
    val tablePath = carbonTable.getTablePath

    // batch load
    var qry: StreamingQuery = null
    val readSocketDF = spark.readStream
      .format("socket")
      .option("host", args(0))
      .option("port", args(1))
      .load()

    // Write data from socket stream to carbondata file
    qry = readSocketDF.writeStream
      .format("carbondata")
      .trigger(ProcessingTime("5 seconds"))
      .option("checkpointLocation", CarbonTablePath.getStreamingCheckpointDir(tablePath))
      .option("dbName", "default")
      .option("tableName", args(3))
      .option(CarbonStreamParser.CARBON_STREAM_PARSER,CarbonStreamParser.CARBON_STREAM_PARSER_CSV)
      .start()

     // start new thread to show data
   //  new Thread() {
   //    override def run(): Unit = {
   //     do {
   //       spark.sql("select count(1) from " + args(3)).show(false)
   //       Thread.sleep(10000)
   //     } while (true)
   //   }
   // }.start()

    qry.awaitTermination()

  }
}