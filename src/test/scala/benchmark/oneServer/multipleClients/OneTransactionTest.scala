package benchmark.oneServer.multipleClients

import benchmark.utils.writer.TransactionDataWriter
import benchmark.utils.{Installer, Launcher}

import scala.collection.mutable._

object OneTransactionTest extends Launcher {
  override val streamName = "stream"
  override val clients = 2
  private val txnCount = 100000
  private val dataSize = 1000
  private val clientThreads = ArrayBuffer[Thread]()
  private val rand = new scala.util.Random()

  def main(args: Array[String]) {
    launch()
    System.exit(0)
  }

  override def launchClients(): Unit = {
    (1 to clients).foreach(x => {
      val thread = new Thread(new Runnable {
        override def run(): Unit = {
          val filename = rand.nextInt(100) + s"TransactionDataWriterTo${x}PartitionOSMC.csv"
          new TransactionDataWriter(streamName, x).run(txnCount, dataSize, filename)
        }
      })
      clientThreads.+=(thread)
    })
    clientThreads.foreach(_.start())
    clientThreads.foreach(_.join())
  }
}

object OneTransactionClient extends Installer {
  private val rand = new scala.util.Random()
  private val txnCount = 1000000
  private val dataSize = 1000
  private val streamName = "stream"
  private val partition = 1

  def main(args: Array[String]) {
    val filename = rand.nextInt(100) + s"TransactionDataWriterTo${partition}PartitionOSMC.csv"
    new TransactionDataWriter(streamName, partition).run(txnCount, dataSize, filename)
    System.exit(0)
  }
}