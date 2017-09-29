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

  final def putRecordsParallel(
                                records: Array[(Array[Byte], Array[Byte])],
                                threadNumber: Int,
                                shift: Int
                              ): Future[Boolean] = {
    val totalSize = records.length
    val batchSize = (totalSize + shift * (threadNumber - 1)) / threadNumber

    val batches = (0 to threadNumber - 1)
      .map(
        i => i * (batchSize - shift)
      )
      .map(
        start => records.slice(start, start + batchSize)
      )

    val futures =
      batches.map(recordsSet =>
        Future(putRecords(recordsSet))
      )

    Future.reduceLeft(
      futures.toIndexedSeq
    )(_ && _)
  }

  final def putRecordsParallelAndGetExecutionTime(
                                                   records: Array[(Array[Byte], Array[Byte])],
                                                   threadNumber: Int = concurrencyLevel,
                                                   shift: Int = 0
                                                 ): Future[Long] = {
    val currentTime =
      System.currentTimeMillis()

    putRecordsParallel(records, threadNumber, shift)
      .map(_ =>
        System.currentTimeMillis() - currentTime
      )
  }
}
