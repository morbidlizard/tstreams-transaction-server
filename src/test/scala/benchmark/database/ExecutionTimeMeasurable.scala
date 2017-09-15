package benchmark.database

trait ExecutionTimeMeasurable {
  def measureTime[T](body: => T): (T, Long) = {
    val currentTime = System.currentTimeMillis()
    val result = body
    val afterTime = System.currentTimeMillis()
    (result, afterTime - currentTime)
  }
}
