package benchmark.database

import java.io.File
import java.io.FileWriter

import StatisticCollector._

import scala.concurrent.Await
import scala.concurrent.duration._

private object StatisticCollector {
  val readTestType = "read"
  val readTestRangeScan = "range_scan"

  val writeTestType = "write"
  val writeTestBatchSync = "batch_sync"
  val writeTestBatchAsync = "batch_async"
}

class StatisticCollector(path: String = "/tmp/benchmark") {

  private def createFile(dbName: String,
                         testType: String,
                         fileName: String): File = {
    val targetFile = new File(s"$path/$dbName/$testType/$fileName.csv")
    val parent = targetFile.getParentFile
    parent.mkdirs()
    targetFile
  }


  def collectSyncWriteStatistics(databaseForBenchmark: AllInOneMeasurable,
                                 records: Array[(Array[Byte], Array[Byte])],
                                 times: Int): Unit = {
    val statistics = Array.fill(times) {
      databaseForBenchmark
        .dropAllRecords()
      val recordsNumber =
        records.length
      val executionTime =
        databaseForBenchmark
          .putRecordsAndGetExecutionTime(records)
          ._2
      (recordsNumber, executionTime)
    }

    val outPutData = statistics
      .map(x => s"${x._1}\t${x._2}")
      .mkString("\n")

    val dbName =
      databaseForBenchmark.toString

    val file =
      createFile(
        dbName,
        writeTestType,
        s"${writeTestBatchSync}_${records.length.toString}"
      )

    val writer =
      new FileWriter(file)

    writer.write(outPutData)
    writer.close()
  }

  def collectAsyncWriteStatistics(databaseForBenchmark: AllInOneMeasurable,
                                  records: Array[(Array[Byte], Array[Byte])],
                                  times: Int): Unit = {
    val statistics = Array.fill(times) {
      databaseForBenchmark
        .dropAllRecords()
      val recordsNumber =
        records.length
      val executionTime =
        Await.result(
          databaseForBenchmark
            .putRecordsParallelAndGetExecutionTime(records),
          5.minutes
        )
      (recordsNumber, executionTime)
    }

    val outPutData = statistics
      .map(x => s"${x._1}\t${x._2}")
      .mkString("\n")

    val dbName =
      databaseForBenchmark.toString

    val file =
      createFile(
        dbName,
        writeTestType,
        s"${writeTestBatchAsync}_${records.length.toString}"
      )

    val writer =
      new FileWriter(file)

    writer.write(outPutData)
    writer.close()
  }



  def collectReadStatistics(databaseForBenchmark: AllInOneMeasurable,
                            records: Array[(Array[Byte], Array[Byte])],
                            recordsNumbersToReadPerScan: Seq[Int],
                            times: Int): Unit = {
    databaseForBenchmark.dropAllRecords()
    databaseForBenchmark.putRecords(records)

    val statistics: Seq[(Int, Long)] =
      recordsNumbersToReadPerScan.flatMap { numberToRead =>
          Array.fill(times) {
            val scannedRecordsAndExecutionTime =
              databaseForBenchmark
                .readRecordsByBatch(numberToRead)
            scannedRecordsAndExecutionTime
          }
      }.flatten

    val outPutData = statistics
      .map(x => s"${x._1}\t${x._2}")
      .mkString("\n")

    val dbName =
      databaseForBenchmark.toString

    val file =
      createFile(
        dbName,
        readTestType,
        readTestRangeScan
      )

    val writer =
      new FileWriter(file)

    writer.write(outPutData)
    writer.close()
  }
}
