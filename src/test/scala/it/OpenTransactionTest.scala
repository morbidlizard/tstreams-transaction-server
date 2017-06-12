package it

import java.io.File
import java.nio.file.Files
import java.util.concurrent.{CountDownLatch, TimeUnit}

import com.bwsw.tstreamstransactionserver.netty.server.transactionIDService.TransactionIDService
import com.bwsw.tstreamstransactionserver.options.{ClientBuilder, ServerBuilder, ServerOptions}
import com.bwsw.tstreamstransactionserver.rpc.{ProducerTransaction, TransactionStates}
import org.apache.commons.io.FileUtils
import org.scalatest.{BeforeAndAfterEach, FlatSpec, Matchers}
import util.Utils

import scala.concurrent.Await
import scala.concurrent.duration._

class OpenTransactionTest extends FlatSpec
  with Matchers
  with BeforeAndAfterEach {


  private val serverStorageOptions = ServerOptions.StorageOptions(
    path = Files.createTempDirectory("dbs_").toFile.getPath
  )
  private val commitLogToBerkeleyDBTaskDelayMs = 100
  private val serverBuilder = new ServerBuilder()
    .withCommitLogOptions(ServerOptions.CommitLogOptions(
      closeDelayMs = commitLogToBerkeleyDBTaskDelayMs
    ))
    .withServerStorageOptions(serverStorageOptions)

  private val clientBuilder = new ClientBuilder()

  private val rand = scala.util.Random
  private def getRandomStream = Utils.getRandomStream
  val secondsWait = 5

  override def beforeEach(): Unit = {
    FileUtils.deleteDirectory(new File(serverStorageOptions.path + java.io.File.separatorChar + serverStorageOptions.metadataDirectory))
    FileUtils.deleteDirectory(new File(serverStorageOptions.path + java.io.File.separatorChar + serverStorageOptions.dataDirectory))
    FileUtils.deleteDirectory(new File(serverStorageOptions.path + java.io.File.separatorChar + serverStorageOptions.commitLogRocksDirectory))
    FileUtils.deleteDirectory(new File(serverStorageOptions.path + java.io.File.separatorChar + serverStorageOptions.commitLogRawDirectory))
  }

  override def afterEach() {
    FileUtils.deleteDirectory(new File(serverStorageOptions.path + java.io.File.separatorChar + serverStorageOptions.metadataDirectory))
    FileUtils.deleteDirectory(new File(serverStorageOptions.path + java.io.File.separatorChar + serverStorageOptions.dataDirectory))
    FileUtils.deleteDirectory(new File(serverStorageOptions.path + java.io.File.separatorChar + serverStorageOptions.commitLogRocksDirectory))
    FileUtils.deleteDirectory(new File(serverStorageOptions.path + java.io.File.separatorChar + serverStorageOptions.commitLogRawDirectory))
  }

  implicit object ProducerTransactionSortable extends Ordering[ProducerTransaction] {
    override def compare(x: ProducerTransaction, y: ProducerTransaction): Int = {
      if (x.stream > y.stream) 1
      else if (x.stream < y.stream) -1
      else if (x.partition > y.partition) 1
      else if (x.partition < y.partition) -1
      else if (x.transactionID > y.transactionID) 1
      else if (x.transactionID < y.transactionID) -1
      else if (x.state.value < y.state.value) -1
      else if (x.state.value > y.state.value) 1
      else 0
    }
  }

  it should "put producer 'opened' transactions and should get them all" in {
    val bundle = Utils.startTransactionServerAndClient(serverBuilder, clientBuilder)

    val stream = getRandomStream
    val streamID = Await.result(bundle.client.putStream(stream), secondsWait.seconds)

    val ALL = 80

    val partition = 1

    val from = TransactionIDService.getTransaction()

    val transactions = (0 until ALL).map { _ =>
      bundle.client.openTransaction(streamID, partition, 24000L)
    }


    Thread.sleep(2000)
    val to = TransactionIDService.getTransaction()

    val res = Await.result(bundle.client.scanTransactions(
      streamID, partition, from, to, Int.MaxValue, Set()
    ), secondsWait.seconds)

    bundle.close()

    res.producerTransactions.size shouldBe transactions.size
    res.producerTransactions.forall(_.state == TransactionStates.Opened) shouldBe true
  }

  it should "open producer transactions and then checkpoint them and should get them all" in {
    val bundle = Utils.startTransactionServerAndClient(serverBuilder, clientBuilder)

    val stream = getRandomStream
    val streamID = Await.result(bundle.client.putStream(stream), secondsWait.seconds)

    val ALL = 80

    val partition = 1

    val transactions = (0 until ALL).map { _ =>
      val id = Await.result(
        bundle.client.openTransaction(streamID, partition, 24000L),
        secondsWait.seconds
      )

      bundle.client.putProducerState(
        ProducerTransaction(streamID, partition, id, TransactionStates.Checkpointed, 0, 25000L)
      )
      id
    }

    val latch1 = new CountDownLatch(1)
    bundle.transactionServer.notifyProducerTransactionCompleted(producerTransaction =>
      producerTransaction.transactionID == transactions.last,
      latch1.countDown()
    )

    latch1.await(secondsWait, TimeUnit.SECONDS)

    val res = Await.result(bundle.client.scanTransactions(
      streamID, partition, transactions.head, transactions.last, Int.MaxValue, Set(TransactionStates.Opened)
    ), secondsWait.seconds)

    bundle.close()

    res.producerTransactions.size shouldBe transactions.size
    res.producerTransactions.forall(_.state == TransactionStates.Checkpointed) shouldBe true
  }

  it should "put producer 'opened' transaction and get notification of it." in {
    val bundle = Utils.startTransactionServerAndClient(serverBuilder, clientBuilder)

    val stream = getRandomStream
    val streamID = Await.result(bundle.client.putStream(stream), secondsWait.seconds)

    val latch = new CountDownLatch(1)
    bundle.transactionServer
      .notifyProducerTransactionCompleted(producerTransaction =>
        producerTransaction.state == TransactionStates.Opened, latch.countDown()
      )

    bundle.client.openTransaction(streamID, 1, 25000)

    latch.await(secondsWait, TimeUnit.SECONDS) shouldBe true
    bundle.close()
  }

}
