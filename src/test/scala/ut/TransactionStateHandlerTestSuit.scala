package ut

import com.bwsw.tstreamstransactionserver.netty.server.transactionMetadataService.ProducerTransactionKey
import com.bwsw.tstreamstransactionserver.netty.server.transactionMetadataService.stateHandler.TransactionStateHandler
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import transactionService.rpc.TransactionStates

class TransactionStateHandlerTestSuit extends FlatSpec with Matchers with BeforeAndAfterAll {
  //arrange
  val transactionStateHandler = new TransactionStateHandler {}
  val ts = 640836800000L
  val openedTTL = 2
  val quantity = -1
  val streamName = "stream_test"
  val streamPartitions = 1
  val streamNameToLong = streamName.hashCode.toLong

  it should "not put producerTransaction with state: Checkpointed. " +
    "It should throw an exception (due an invalid transition of state machine)" in {

    val producerTransactionKey = createProducerTransactionKey(TransactionStates.Checkpointed, ts)

    assertThrows[IllegalArgumentException] {
      transactionStateHandler.transitProducerTransactionToNewState(Seq(producerTransactionKey))
    }
  }

  it should "not put producerTransaction with state: Invalid. " +
    "It should throw an exception (due an invalid transition of state machine)" in {

    val producerTransactionKey = createProducerTransactionKey(TransactionStates.Invalid, ts)

    assertThrows[IllegalArgumentException] {
      transactionStateHandler.transitProducerTransactionToNewState(Seq(producerTransactionKey))
    }
  }

  it should "not put producerTransaction with state: Cancel. " +
    "It should throw an exception (due an invalid transition of state machine)" in {

    val producerTransactionKey = createProducerTransactionKey(TransactionStates.Cancel, ts)

    assertThrows[IllegalArgumentException] {
      transactionStateHandler.transitProducerTransactionToNewState(Seq(producerTransactionKey))
    }
  }

  it should "process the following chain of states of producer transactions: Opened -> Checkpointed" in {
    val openedProducerTransactionKey = createProducerTransactionKey(TransactionStates.Opened, ts)
    val checkpointedProducerTransactionKey = createProducerTransactionKey(TransactionStates.Checkpointed, ts + 1)

    transactionStateHandler.transitProducerTransactionToNewState(Seq(openedProducerTransactionKey, checkpointedProducerTransactionKey))
  }

  it should "process the following chain of states of producer transactions: Opened -> Updated -> Updated -> Checkpointed" in {
    val openedProducerTransactionKey = createProducerTransactionKey(TransactionStates.Opened, ts)
    val updatedProducerTransactionKey1 = createProducerTransactionKey(TransactionStates.Opened, ts + 1)
    val updatedProducerTransactionKey2 = createProducerTransactionKey(TransactionStates.Opened, ts + 2)
    val checkpointedProducerTransactionKey = createProducerTransactionKey(TransactionStates.Checkpointed, ts + 3)

    transactionStateHandler.transitProducerTransactionToNewState(
      Seq(openedProducerTransactionKey,
        updatedProducerTransactionKey1,
        updatedProducerTransactionKey2,
        checkpointedProducerTransactionKey
      ))
  }

  it should "process the following chain of states of producer transactions: Opened -> Updated -> Updated -> Cancel" in {
    val openedProducerTransactionKey = createProducerTransactionKey(TransactionStates.Opened, ts)
    val updatedProducerTransactionKey1 = createProducerTransactionKey(TransactionStates.Opened, ts + 1)
    val updatedProducerTransactionKey2 = createProducerTransactionKey(TransactionStates.Opened, ts + 2)
    val checkpointedProducerTransactionKey = createProducerTransactionKey(TransactionStates.Cancel, ts + 3)

    transactionStateHandler.transitProducerTransactionToNewState(
      Seq(openedProducerTransactionKey,
        updatedProducerTransactionKey1,
        updatedProducerTransactionKey2,
        checkpointedProducerTransactionKey
      ))
  }

  private def createProducerTransactionKey(transactionState: TransactionStates, ts: Long) = {
    val producerTransaction = transactionService.rpc.ProducerTransaction(streamName, streamPartitions, ts, transactionState, quantity, openedTTL)

    ProducerTransactionKey(producerTransaction, streamNameToLong, ts)
  }
}