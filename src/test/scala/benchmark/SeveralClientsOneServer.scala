package benchmark

import benchmark.OneClientOneServerTest._
import scala.collection.mutable._

object SeveralClientsOneServer {
  private val txnCount = 1000
  private val dataSize = 1000
  private val clients = 2
  private val clientThreads = ArrayBuffer[Thread]()

  def main(args: Array[String]) {
    clearDB()
    startAuthServer()
    startTransactionServer()
    (1 to clients).foreach(x => {
      val thread = new Thread(new Runnable {
        override def run(): Unit = ClientWriterToOnePartition.main(Array(txnCount.toString, dataSize.toString))
      })
      clientThreads.+=(thread)
    })
    clientThreads.foreach(_.start())
    clientThreads.foreach(_.join())
    System.exit(0)
  }
}