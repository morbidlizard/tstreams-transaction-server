package netty.server

trait CheckpointTTL {
  val streamTTL = new java.util.concurrent.ConcurrentHashMap[String, netty.server.streamService.KeyStream]()
  def getStreamDatabaseObject(stream: String): netty.server.streamService.KeyStream
}
