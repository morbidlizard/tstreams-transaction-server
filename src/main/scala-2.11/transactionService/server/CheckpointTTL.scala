package transactionService.server

trait CheckpointTTL {
  val streamTTL = new java.util.concurrent.ConcurrentHashMap[String, transactionService.server.streamService.KeyStream]()
  def getStreamDatabaseObject(stream: String): transactionService.server.streamService.KeyStream
}
