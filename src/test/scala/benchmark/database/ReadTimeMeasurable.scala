package benchmark.database

trait ReadTimeMeasurable
  extends ExecutionTimeMeasurable {

  def readALLRecords(): Array[(Array[Byte], Array[Byte])]

  def readRecords(from: Array[Byte],
                  to: Array[Byte]): Array[(Array[Byte], Array[Byte])]

  final def readRecordsByBatch(number: Int): Array[(Int, Long)] = {
    val records = readALLRecordsAndGetExecutionTime()._1
    records
      .grouped(number)
      .map{recordsSet => (recordsSet.head, recordsSet.last)}
      .map{case (fromRecord, toRecord) =>
        measureTime(readRecords(fromRecord._1, toRecord._1))
      }
      .map{case (records, executionTime) =>
        (records.length, executionTime)
      }
      .toArray
  }

  final def readALLRecordsAndGetExecutionTime(): (Array[(Array[Byte], Array[Byte])], Long) = {
    measureTime(readALLRecords())
  }
}
