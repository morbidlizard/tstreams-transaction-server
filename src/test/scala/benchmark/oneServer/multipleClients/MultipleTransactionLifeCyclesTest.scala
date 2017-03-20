package benchmark.oneServer.multipleClients

import benchmark.utils.Launcher
import benchmark.utils.writer.TransactionLifeCycleWriter

import scala.collection.mutable.ArrayBuffer

object MultipleTransactionLifeCyclesTest extends Launcher {
  override val streamName = "stream"
  override val clients = 2
  private val txnCount = 1000000
  private val dataSize = 1
  private val clientThreads = ArrayBuffer[Thread]()
  private val rand = new scala.util.Random()

  def main(args: Array[String]) {
   // launch()
    createStream(streamName, clients)
    launchClients()
    System.exit(0)
  }

  override def launchClients(): Unit = {
    (1 to clients).foreach(x => {
      val thread = new Thread(new Runnable {
        override def run(): Unit = {
          val filename = rand.nextInt(100) + s"_${txnCount}TransactionLifeCycleWriterOSMC.csv"
          new TransactionLifeCycleWriter(streamName, x).run(txnCount, dataSize, filename)
        }
      })
      clientThreads.+=(thread)
    })
    clientThreads.foreach(_.start())
    clientThreads.foreach(_.join())
  }
}
