package benchmark.oneServer.multipleClients

import benchmark.utils.{Installer, Launcher}
import benchmark.utils.writer.TransactionMetadataWriter

import scala.collection.mutable.ArrayBuffer

object MultipleTransactionsTest extends Launcher {
  override val streamName = "stream"
  override val clients = 16
  private val txnCount = 1000000
  private val rand = new scala.util.Random()
  private val clientThreads = ArrayBuffer[Thread]()

  def main(args: Array[String]) {
    launch()
    System.exit(0)
  }

  override def launchClients(): Unit = {
    (1 to clients).foreach(x => {
      val thread = new Thread(new Runnable {
        override def run(): Unit = {
          val filename = rand.nextInt(100) + s"_${txnCount}TransactionMetadataWriterOSMC.csv"
          new TransactionMetadataWriter(streamName, x).run(txnCount, filename)
        }
      })
      clientThreads.+=(thread)
    })
    clientThreads.foreach(_.start())
    clientThreads.foreach(_.join())
  }
}

object MultipleTransactionsClient extends Installer {
  private val rand = new scala.util.Random()
  private val txnCount = 1000000
  private val streamName = "stream"
  private val partition = 1

  def main(args: Array[String]) {
    val filename = rand.nextInt(100) + s"_${txnCount}TransactionMetadataWriterOSMC.csv"
    new TransactionMetadataWriter(streamName, partition).run(txnCount, filename)
    System.exit(0)
  }
}


import com.sleepycat.je._
import java.io.File
import java.nio.ByteBuffer

object Main {
  def main(args: Array[String]) = {
    val envConf = new EnvironmentConfig()
    envConf.setAllowCreate(true)
    envConf.setDurability(Durability.COMMIT_NO_SYNC)
    envConf.setTransactional(true)
    envConf.setConfigParam("je.log.fileMax", "100000000")

    val env = new Environment(new File("/tmp/test/"), envConf)

    val dbConf = new DatabaseConfig()
    dbConf.setAllowCreate(true)
    dbConf.setTransactional(true)

    val myDb = env.openDatabase(null, "db", dbConf)
    val myDbOpen = env.openDatabase(null, "db_open", dbConf)

    val txnConf = new TransactionConfig()
   // txnConf.setDurability(Durability.COMMIT_NO_SYNC)

    val begin = System.currentTimeMillis()
    (0 until 1000000).foreach(itm => {
      val k = new DatabaseEntry(ByteBuffer.allocate(32).putLong(itm).array())
      val v = new DatabaseEntry(ByteBuffer.allocate(16).putLong(itm).array())
      val k1 = new DatabaseEntry(ByteBuffer.allocate(32).putLong(itm).array())
      val v1 = new DatabaseEntry(ByteBuffer.allocate(16).putInt(1).array())

      val txn = env.beginTransaction(null, txnConf)
      myDb.put(txn, k, v)
      myDbOpen.put(txn, k1, v1)
      txn.commit()
//
//      myDb.get(null, k, v1, LockMode.DEFAULT)
//      println(v.getData.sameElements(v1.getData))
    })
    val end = System.currentTimeMillis()
    println(end - begin)

    myDb.close()
    myDbOpen.close()
    env.close()
  }
}