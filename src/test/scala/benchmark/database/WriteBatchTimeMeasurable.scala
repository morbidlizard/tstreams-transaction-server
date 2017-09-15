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


  final def putRecordsAndGetExecutionTime(records: Array[(Array[Byte], Array[Byte])]): (Boolean, Long) = {
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

  final def putRecordsParallelAndGetExecutionTime(records: Array[(Array[Byte], Array[Byte])]): Future[Long] = {
    val currentTime =
      System.currentTimeMillis()

    putRecordsParallel(records)
      .map(_ =>
        System.currentTimeMillis() - currentTime
      )
  }
}
