package benchmark.database

import WriteBatchTimeMeasurable._

import scala.concurrent.ExecutionContext
import scala.concurrent.Future


private object WriteBatchTimeMeasurable {
  private val concurrencyLevel =
    java.lang.Runtime.getRuntime.availableProcessors()

  implicit val context: ExecutionContext =
    scala.concurrent.ExecutionContext.Implicits.global
}


trait WriteBatchTimeMeasurable
  extends ExecutionTimeMeasurable
{

  def putRecords(records: Array[(Array[Byte], Array[Byte])]): Boolean


  final def putRecordsAndDisplayExecutionTime(records: Array[(Array[Byte], Array[Byte])]): Boolean = {
    measureTime(putRecords(records))
  }

  final def putRecordsParallel(records: Array[(Array[Byte], Array[Byte])]): Future[Boolean] = {
    val batches =
      records.grouped(records.length / concurrencyLevel)

    val futures =
      batches.map(recordsSet =>
        Future(putRecords(recordsSet))
      )

    Future.reduceLeft(
      futures.toIndexedSeq
    )(_ && _)
  }

  final def putRecordsParallelAndDisplayExecutionTime(records: Array[(Array[Byte], Array[Byte])]): Future[Unit] = {
    val currentTime =
      System.currentTimeMillis()

    putRecordsParallel(records)
      .map(_ =>
        println(System.currentTimeMillis() - currentTime)
      )
  }
}
