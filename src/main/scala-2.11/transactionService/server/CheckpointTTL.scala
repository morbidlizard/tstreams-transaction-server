package transactionService.server

trait CheckpointTTL {
  val streamTTL = new java.util.concurrent.ConcurrentHashMap[String, transactionService.server.streamService.Stream]()
  def getStream(stream: String): transactionService.server.streamService.Stream
}
