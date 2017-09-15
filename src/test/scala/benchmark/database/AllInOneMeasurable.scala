package benchmark.database

import java.io.Closeable

trait AllInOneMeasurable
  extends WriteBatchTimeMeasurable
  with ReadTimeMeasurable
  with Closeable {

  def dropAllRecords(): Unit
}
