package benchmark.database

trait ExecutionTimeMeasurable {
  def measureTime[T](body: => T): T = {
    val currentTime = System.currentTimeMillis()
    val result = body
    val afterTime = System.currentTimeMillis()
    println(afterTime - currentTime)
    result
  }
}
