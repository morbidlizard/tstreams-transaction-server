//package it.multinode
//
//import com.bwsw.tstreamstransactionserver.rpc.{ConsumerTransaction, ProducerTransaction, TransactionStates}
//import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
//import util.Utils
//
//import scala.concurrent.Await
//
//class CommonCheckpointGroupHandlerRouterTest
//  extends FlatSpec
//    with BeforeAndAfterAll
//    with Matchers
//{
//
//  private val rand = scala.util.Random
//
//  private def getRandomStream =
//    new com.bwsw.tstreamstransactionserver.rpc.StreamValue {
//      override val name: String = rand.nextInt(10000).toString
//      override val partitions: Int = rand.nextInt(10000)
//      override val description: Option[String] = if (rand.nextBoolean()) Some(rand.nextInt(10000).toString) else None
//      override val ttl: Long = Long.MaxValue
//      override val zkPath: Option[String] = None
//    }
//
//  private def chooseStreamRandomly(streams: IndexedSeq[com.bwsw.tstreamstransactionserver.rpc.StreamValue]) = streams(rand.nextInt(streams.length))
//
//  private def getRandomProducerTransaction(streamID: Int,
//                                           streamObj: com.bwsw.tstreamstransactionserver.rpc.StreamValue,
//                                           transactionState: TransactionStates = TransactionStates(rand.nextInt(TransactionStates.list.length) + 1),
//                                           id: Long = System.nanoTime()) =
//    new ProducerTransaction {
//      override val transactionID: Long = id
//      override val state: TransactionStates = transactionState
//      override val stream: Int = streamID
//      override val ttl: Long = Long.MaxValue
//      override val quantity: Int = 0
//      override val partition: Int = streamObj.partitions
//    }
//
//  private def getRandomConsumerTransaction(streamID: Int, streamObj: com.bwsw.tstreamstransactionserver.rpc.StreamValue) =
//    new ConsumerTransaction {
//      override val transactionID: Long = scala.util.Random.nextLong()
//      override val name: String = rand.nextInt(10000).toString
//      override val stream: Int = streamID
//      override val partition: Int = streamObj.partitions
//    }
//
//
//  val secondsWait = 5
//
//
//  it should "[scanTransactions] put transactions and get them back" in {
//    val bundle = Utils.startTransactionServerAndClient(
//      zkClient, serverBuilder, clientBuilder
//    )
//
//    bundle.operate { transactionServer =>
//      val client = bundle.client
//      val stream = getRandomStream
//      val streamID = Await.result(client.putStream(stream), secondsWait.seconds)
//
//      val producerTransactions = Array.fill(30)(getRandomProducerTransaction(streamID, stream)).filter(_.state == TransactionStates.Opened) :+
//        getRandomProducerTransaction(streamID, stream).copy(state = TransactionStates.Opened)
//
//      val consumerTransactions = Array.fill(100)(getRandomConsumerTransaction(streamID, stream))
//
//      Await.result(client.putTransactions(producerTransactions, consumerTransactions), secondsWait.seconds)
//
//      val statesAllowed = Array(TransactionStates.Opened, TransactionStates.Updated)
//      val (from, to) = (
//        producerTransactions.filter(txn => statesAllowed.contains(txn.state)).minBy(_.transactionID).transactionID,
//        producerTransactions.filter(txn => statesAllowed.contains(txn.state)).maxBy(_.transactionID).transactionID
//      )
//
//      TestTimer.updateTime(TestTimer.getCurrentTime + maxIdleTimeBetweenRecordsMs)
//
//      Await.result(client.putConsumerCheckpoint(getRandomConsumerTransaction(streamID, stream)), secondsWait.seconds)
//      transactionServer.scheduledCommitLog.run()
//      transactionServer.commitLogToRocksWriter.run()
//
//
//      val resFrom_1From = Await.result(client.scanTransactions(streamID, stream.partitions, from - 1, from, Int.MaxValue, Set()), secondsWait.seconds)
//      resFrom_1From.producerTransactions.size shouldBe 1
//      resFrom_1From.producerTransactions.head.transactionID shouldBe from
//
//
//      val resFromFrom = Await.result(client.scanTransactions(streamID, stream.partitions, from, from, Int.MaxValue, Set()), secondsWait.seconds)
//      resFromFrom.producerTransactions.size shouldBe 1
//      resFromFrom.producerTransactions.head.transactionID shouldBe from
//
//
//      val resToFrom = Await.result(client.scanTransactions(streamID, stream.partitions, to, from, Int.MaxValue, Set()), secondsWait.seconds)
//      resToFrom.producerTransactions.size shouldBe 0
//
//      val producerTransactionsByState = producerTransactions.groupBy(_.state)
//      val res = Await.result(client.scanTransactions(streamID, stream.partitions, from, to, Int.MaxValue, Set()), secondsWait.seconds).producerTransactions
//
//      val producerOpenedTransactions = producerTransactionsByState(TransactionStates.Opened).sortBy(_.transactionID)
//
//      res.head shouldBe producerOpenedTransactions.head
//      res shouldBe sorted
//    }
//  }
//
//
//}
