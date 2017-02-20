package benchmark.utils

trait Launcher extends Installer {
  protected val streamName: String
  protected val clients: Int

  def launch() = {
    clearDB()
    startTransactionServer()
    Thread.sleep(2000L)
    createStream(streamName, clients)
    launchClients()
    deleteStream(streamName)
  }

  protected def launchClients(): Unit
}