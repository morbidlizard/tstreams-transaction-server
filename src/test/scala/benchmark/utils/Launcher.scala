package benchmark.utils

trait Launcher extends Installer {
  protected val streamName: String
  protected val clients: Int

  def launch() = {
    clearDB()
    startTransactionServer()
    Thread.sleep(2000L)
    val streamID = createStream(streamName, clients)
    launchClients(streamID)
    deleteStream(streamName)
  }

  protected def launchClients(streamID: Int): Unit
}
