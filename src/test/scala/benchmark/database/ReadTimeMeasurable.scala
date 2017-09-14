package benchmark.database

trait ReadTimeMeasurable
  extends ExecutionTimeMeasurable {

  def readALLRecords(): Array[(Array[Byte], Array[Byte])]

  def readRecords(from: Array[Byte],
                  to: Array[Byte]): Array[(Array[Byte], Array[Byte])]

  final def readRecordsByBatch(number: Int): Unit = {
    val records = readALLRecordsAndDisplayExecutionTime()
    records
      .grouped(number)
      .map{recordsSet => (recordsSet.head, recordsSet.last)}
      .foreach{range => measureTime(readRecords(range._1._1, range._2._1))}
  }

  final def readALLRecordsAndDisplayExecutionTime(): Array[(Array[Byte], Array[Byte])] = {
    measureTime(readALLRecords())
  }
}
