package benchmark
import scala.collection.mutable._

object MultipleClientsOneTransactionOneServer extends Installer {
  private val streamName = "stream"
  private val txnCount = 100000
  private val dataSize = 1000
  private val clients = 2
  private val clientThreads = ArrayBuffer[Thread]()

  def main(args: Array[String]) {
    clearDB()
    startAuthServer()
    startTransactionServer()
    createStream(streamName, clients)
    launchClients()
    deleteStream(streamName)
    System.exit(0)
  }

  private def launchClients() = {
    (1 to clients).foreach(x => {
      val thread = new Thread(new Runnable {
        override def run(): Unit = {
          val writer = new ClientWriterToOneTransaction(streamName)
          writer.run(txnCount, dataSize)
        }
      })
      clientThreads.+=(thread)
    })
    clientThreads.foreach(_.start())
    clientThreads.foreach(_.join())
  }
}

object MultipleClientsMultipleTransactionsOneServer extends Installer {
  private val streamName = "stream"
  private val txnCount = 1000000
  private val dataSize = 1
  private val clients = 4
  private val clientThreads = ArrayBuffer[Thread]()

  def main(args: Array[String]) {
    clearDB()
    startAuthServer()
    startTransactionServer()
    createStream(streamName, clients)
    launchClients()
    deleteStream(streamName)
    System.exit(0)
  }

  private def launchClients() = {
    (1 to clients).foreach(x => {
      val thread = new Thread(new Runnable {
        override def run(): Unit = {
          val writer = new ClientWriterTransactionsLifeCycle(streamName, x)
          writer.run(txnCount, dataSize)
        }
      })
      clientThreads.+=(thread)
    })
    clientThreads.foreach(_.start())
    clientThreads.foreach(_.join())
  }
}