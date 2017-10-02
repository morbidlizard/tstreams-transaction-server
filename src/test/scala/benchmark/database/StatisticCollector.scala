package benchmark.database

import sys.process._

import java.io.{File, FileWriter}
import java.nio.file.{Files, Paths}

import benchmark.database.StatisticCollector._
import benchmark.database.misc.{Arithmetic, ProgressBar}

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.language.implicitConversions
import benchmark.database.misc.ArithmeticImplicits._

import scala.reflect.ClassTag

private object StatisticCollector {
  val readTestType = "read"
  val readTestRangeScan = "range_scan"

  val writeTestType = "write"
  val writeTestBatchSync = "batch_sync"
  val writeTestBatchAsync = "batch_async"
}

class StatisticCollector(path: String = "/home/rakhimovvv/trans/bm/output") {

  private def clearPath(): Unit = {
    // remove everything from the folder
    val directory = new File(path)
    directory.mkdirs()
    require(directory.isDirectory)
    directory.listFiles.foreach(x => x.delete())
  }

  clearPath()


  private def appendToFile(file: File, values: Any*): Unit ={
    val writer = new FileWriter(file, true)

    writer.write(values.mkString("\t"))
    writer.write("\n")

    writer.close()
  }

  private def openFile(testType: String,
                         sizes: Seq[Int]): File = {
    val fullPath = s"$path/$testType.csv"
    val append = Files.exists(Paths.get(fullPath))
    val targetFile = new File(fullPath)

    if (!append) {
      appendToFile(targetFile, (Seq("", "") ++ sizes):_*)
    } else{
      appendToFile(targetFile)
    }

    targetFile
  }

  def collectStatistics[T<%Arithmetic[T]](databaseForBenchmark: AllInOneMeasurable,
                        testType: String,
                        fileName: String,
                        sizes: Seq[Int],
                        operation: (Int) => Array[T],
                        times: Int,
                        init: Option[() => Unit] = None,
                        verbose: Boolean = true)(implicit classTag: ClassTag[T])
  : Unit = {

    val dbName =
      databaseForBenchmark.toString

    if (verbose) println(s"$dbName $testType")

    init match {
      case Some(func) => func()
      case None =>
    }

    val sizeStatistics = sizes.map(
      size => {
        if (verbose) println(s"\tsize: $size")
        val progressBar = new ProgressBar(times, 1)
        var i = 1
        val statistics = Array.fill(times) {
          val result = operation(size)
          i += 1
          if (verbose) {
            progressBar.set(i)
          }
          result
        }.flatten

        if (verbose) progressBar.finish()

        val sum = statistics
          .reduce(_ + _)
        val mean = sum / statistics.length

        val deviation = statistics
          .map(_ - mean)
          .map(x => x * x)
          .reduce(_ + _) / statistics.length

        val devSqrt = deviation ** .5

        (size, mean, devSqrt)
      }
    )

    val outputData = (1 to 2)
      .map(i => {
        Array(
          if (i == 1) dbName else "",
          if (i == 1) "mean" else "std"
        ) ++ sizeStatistics.map(x => if (i == 1) x._2 else x._3)
      })


    val file = openFile(testType, sizes)

    outputData.foreach(
      line => appendToFile(file, line:_*)
    )
  }


  def collectSyncWriteStatistics(databaseForBenchmark: AllInOneMeasurable,
                                 records: Array[(Array[Byte], Array[Byte])],
                                 sizes: Seq[Int],
                                 times: Int)
  : Unit = collectStatistics(
      databaseForBenchmark,
      writeTestType,
      s"${writeTestBatchSync}",
      sizes,
      (size) => {
        databaseForBenchmark
          .dropAllRecords()
        val recordsNumber =
          records.length
        val executionTime =
          databaseForBenchmark
            .putRecordsAndGetExecutionTime(records)
            ._2
            .toDouble
        Array(executionTime)
      },
      times
  )

  def collectAsyncWriteStatistics(databaseForBenchmark: AllInOneMeasurable,
                                  records: Array[(Array[Byte], Array[Byte])],
                                  sizes: Seq[Int],
                                  threadNumber: Int,
                                  shiftRatio: Double,
                                  times: Int)
  : Unit = collectStatistics(
    databaseForBenchmark,
    s"${writeTestType}_${threadNumber}_threads_${shiftRatio}_shift",
    s"${writeTestBatchAsync}",
    sizes,
    (size) => {
      databaseForBenchmark
        .dropAllRecords()
      val someRecords = records.slice(0, size)

      val executionTime =
        Await.result(
          databaseForBenchmark
            .putRecordsParallelAndGetExecutionTime(
              someRecords,
              threadNumber,
              (someRecords.length * shiftRatio).toInt
            ),
          5.minutes
        )
      Array(executionTime.toDouble)
    },
    times
  )

  def collectReadStatistics(databaseForBenchmark: AllInOneMeasurable,
                            records: Array[(Array[Byte], Array[Byte])],
                            sizes: Seq[Int],
                            times: Int)
  : Unit = collectStatistics(
    databaseForBenchmark,
    readTestType,
    readTestRangeScan,
    sizes,
    (size) => {
      val scannedRecordsAndExecutionTime =
        databaseForBenchmark
          .readRecordsByBatch(size)
      scannedRecordsAndExecutionTime.map {
        case (_, y) => y.toDouble
      }
    },
    times,
    Some(() => {
      databaseForBenchmark.dropAllRecords()
      databaseForBenchmark.putRecords(records)
    })
  )
}
