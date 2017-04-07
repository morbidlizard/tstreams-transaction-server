package ut

import com.bwsw.tstreamstransactionserver.netty.server.transactionMetadataService.ProducerTransactionRecord
import com.bwsw.tstreamstransactionserver.netty.server.transactionMetadataService.stateHandler.TransactionStateHandler
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import com.bwsw.tstreamstransactionserver.rpc.TransactionStates
import com.bwsw.tstreamstransactionserver.rpc.TransactionStates._

class TransactionStateHandlerTestSuite extends FlatSpec with Matchers with BeforeAndAfterAll {
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
    //arrange
    val producerTransaction = createProducerTransaction(Checkpointed, ts)

    //act and assert
    assertThrows[IllegalArgumentException] {
      transactionStateHandler.transitProducerTransactionToNewState(Seq(producerTransaction))
    }
  }

  it should "not put producerTransaction with state: Invalid. " +
    "It should throw an exception (due an invalid transition of state machine)" in {
    //arrange
    val producerTransaction = createProducerTransaction(Invalid, ts)

    //act and assert
    assertThrows[IllegalArgumentException] {
      transactionStateHandler.transitProducerTransactionToNewState(Seq(producerTransaction))
    }
  }

  it should "not put producerTransaction with state: Cancel. " +
    "It should throw an exception (due an invalid transition of state machine)" in {
    //arrange
    val producerTransaction = createProducerTransaction(Cancel, ts)

    //act and assert
    assertThrows[IllegalArgumentException] {
      transactionStateHandler.transitProducerTransactionToNewState(Seq(producerTransaction))
    }
  }

  it should "not put producerTransaction with state: Updated. " +
    "It should throw an exception (due an invalid transition of state machine)" in {
    //arrange
    val producerTransaction = createProducerTransaction(Updated, ts)

    //act and assert
    assertThrows[IllegalArgumentException] {
      transactionStateHandler.transitProducerTransactionToNewState(Seq(producerTransaction))
    }
  }

  it should "not process the following chain of states of producer transactions: Opened -> Invalid. " +
    "It should throw an exception (due an invalid transition of state machine)" in {
    //arrange
    val openedProducerTransaction = createProducerTransaction(Opened, ts)
    val invalidProducerTransaction = createProducerTransaction(Invalid, ts + 1)

    //act and assert
    assertThrows[IllegalArgumentException] {
      transactionStateHandler.transitProducerTransactionToNewState(
        Seq(
          openedProducerTransaction,
          invalidProducerTransaction
        ))
    }
  }

  it should "not process the following chain of states of producer transactions: Opened -> Updated -> Updated -> Invalid. " +
    "It should throw an exception (due an invalid transition of state machine)" in {
    //arrange
    val openedProducerTransaction = createProducerTransaction(Opened, ts)
    val updatedProducerTransaction1 = createProducerTransaction(Updated, ts + 1)
    val updatedProducerTransaction2 = createProducerTransaction(Updated, ts + 2)
    val invalidProducerTransaction = createProducerTransaction(Invalid, ts + 3)

    //act and assert
    assertThrows[IllegalArgumentException] {
      transactionStateHandler.transitProducerTransactionToNewState(
        Seq(
          openedProducerTransaction,
          updatedProducerTransaction1,
          updatedProducerTransaction2,
          invalidProducerTransaction
        ))
    }
  }

  it should "process the following chain of states of producer transactions: Opened -> Checkpointed. " +
    "The final state of transaction should be Checkpointed" in {
    //arrange
    val openedProducerTransaction = createProducerTransaction(Opened, ts)
    val checkpointedProducerTransaction = createProducerTransaction(Checkpointed, ts + 1)

    //act
    val finalState = transactionStateHandler.transitProducerTransactionToNewState(
      Seq(openedProducerTransaction,
        checkpointedProducerTransaction
      ))

    //assert
    finalState.state shouldBe Checkpointed
  }

  it should "process the following chain of states of producer transactions: Opened -> Updated -> Updated -> Checkpointed. " +
    "The final state of transaction should be Checkpointed" in {
    //arrange
    val openedProducerTransaction = createProducerTransaction(Opened, ts)
    val updatedProducerTransaction1 = createProducerTransaction(Updated, ts + 1)
    val updatedProducerTransaction2 = createProducerTransaction(Updated, ts + 2)
    val checkpointedProducerTransaction = createProducerTransaction(Checkpointed, ts + 3)

    //act
    val finalState = transactionStateHandler.transitProducerTransactionToNewState(
      Seq(openedProducerTransaction,
        updatedProducerTransaction1,
        updatedProducerTransaction2,
        checkpointedProducerTransaction
      ))

    //assert
    finalState.state shouldBe Checkpointed
  }

  it should "process the following chain of states of producer transactions: Opened -> Updated -> Checkpointed -> Cancel. " +
    "The final state of transaction should be Checkpointed, because it should ignore the part of chain following after Checkpointed state." in {
    //arrange
    val openedProducerTransaction = createProducerTransaction(Opened, ts)
    val updatedProducerTransaction = createProducerTransaction(Updated, ts + 1)
    val checkpointedProducerTransaction = createProducerTransaction(Checkpointed, ts + 2)
    val cancelProducerTransaction = createProducerTransaction(Cancel, ts + 3)

    //act
    val finalState = transactionStateHandler.transitProducerTransactionToNewState(
      Seq(openedProducerTransaction,
        updatedProducerTransaction,
        checkpointedProducerTransaction,
        cancelProducerTransaction
      ))

    //assert
    finalState.state shouldBe Checkpointed
  }

  it should "process the following chain of states of producer transactions: Opened -> Updated -> Checkpointed -> Invalid. " +
    "The final state of transaction should be Checkpointed, because it should ignore the part of chain following after Checkpointed state." in {
    //arrange
    val openedProducerTransaction = createProducerTransaction(Opened, ts)
    val updatedProducerTransaction = createProducerTransaction(Updated, ts + 1)
    val checkpointedProducerTransaction = createProducerTransaction(Checkpointed, ts + 2)
    val cancelProducerTransaction = createProducerTransaction(Invalid, ts + 3)

    //act
    val finalState = transactionStateHandler.transitProducerTransactionToNewState(
      Seq(openedProducerTransaction,
        updatedProducerTransaction,
        checkpointedProducerTransaction,
        cancelProducerTransaction
      ))

    //assert
    finalState.state shouldBe Checkpointed
  }

  it should "process the following chain of states of producer transactions: Opened -> Updated -> Checkpointed -> Opened -> Cancel. " +
    "The final state of transaction should be Checkpointed, because it should ignore the part of chain following after Checkpointed state." in {
    //arrange
    val openedProducerTransaction = createProducerTransaction(Opened, ts)
    val updatedProducerTransaction = createProducerTransaction(Updated, ts + 1)
    val checkpointedProducerTransaction = createProducerTransaction(Checkpointed, ts + 2)
    val openedProducerTransaction2 = createProducerTransaction(Opened, ts + 3)
    val cancelProducerTransaction = createProducerTransaction(Cancel, ts + 4)

    //act
    val finalState = transactionStateHandler.transitProducerTransactionToNewState(
      Seq(openedProducerTransaction,
        updatedProducerTransaction,
        checkpointedProducerTransaction,
        openedProducerTransaction2,
        cancelProducerTransaction
      ))

    //assert
    finalState.state shouldBe Checkpointed
  }

  it should "process the following chain of states of producer transactions: Opened -> Cancel. " +
    "The final state of transaction should be Invalid" in {
    //arrange
    val openedProducerTransaction = createProducerTransaction(Opened, ts)
    val cancelProducerTransaction = createProducerTransaction(Cancel, ts + 1)

    //act
    val finalState = transactionStateHandler.transitProducerTransactionToNewState(
      Seq(openedProducerTransaction,
        cancelProducerTransaction
      ))

    //assert
    finalState.state shouldBe Invalid
  }

  it should "process the following chain of states of producer transactions: Opened -> Updated -> Updated -> Cancel." +
    "The final state of transaction should be Invalid" in {
    //arrange
    val openedProducerTransaction = createProducerTransaction(Opened, ts)
    val updatedProducerTransaction1 = createProducerTransaction(Updated, ts + 1)
    val updatedProducerTransaction2 = createProducerTransaction(Updated, ts + 2)
    val cancelProducerTransaction = createProducerTransaction(Cancel, ts + 3)

    //act
    val finalState = transactionStateHandler.transitProducerTransactionToNewState(
      Seq(openedProducerTransaction,
        updatedProducerTransaction1,
        updatedProducerTransaction2,
        cancelProducerTransaction
      ))

    //assert
    finalState.state shouldBe Invalid
  }

  it should "process the following chain of states of producer transactions: Opened -> Updated -> Cancel -> Checkpointed. " +
    "The final state of transaction should be Invalid because it should ignore the part of chain following after Cancel state" in {
    //arrange
    val openedProducerTransaction = createProducerTransaction(Opened, ts)
    val updatedProducerTransaction1 = createProducerTransaction(Updated, ts + 1)
    val cancelProducerTransaction = createProducerTransaction(Cancel, ts + 2)
    val checkpointedProducerTransaction = createProducerTransaction(Checkpointed, ts + 3)

    //act
    val finalState = transactionStateHandler.transitProducerTransactionToNewState(
      Seq(openedProducerTransaction,
        updatedProducerTransaction1,
        cancelProducerTransaction,
        checkpointedProducerTransaction
      ))

    //assert
    finalState.state shouldBe Invalid
  }

  it should "process the following chain of states of producer transactions: Opened -> Updated -> Cancel -> Opened -> Checkpointed. " +
    "The final state of transaction should be Invalid  because it should ignore the part of chain following after Cancel state" in {
    //arrange
    val openedProducerTransaction = createProducerTransaction(Opened, ts)
    val updatedProducerTransaction1 = createProducerTransaction(Updated, ts + 1)
    val cancelProducerTransaction = createProducerTransaction(Cancel, ts + 2)
    val openedProducerTransaction2 = createProducerTransaction(Opened, ts + 3)
    val checkpointedProducerTransaction = createProducerTransaction(Checkpointed, ts + 4)

    //act
    val finalState = transactionStateHandler.transitProducerTransactionToNewState(
      Seq(openedProducerTransaction,
        updatedProducerTransaction1,
        cancelProducerTransaction,
        openedProducerTransaction2,
        checkpointedProducerTransaction
      ))

    //assert
    finalState.state shouldBe Invalid
  }

  it should "process the case in which a producer transaction remains in Opened state" in {
    //arrange
    val openedProducerTransaction = createProducerTransaction(Opened, ts)

    //act
    val finalState = transactionStateHandler.transitProducerTransactionToNewState(Seq(openedProducerTransaction))

    //assert
    finalState.state shouldBe Opened
  }

  it should "process the following chain of states of producer transactions: Opened -> Updated -> Updated. " +
    "The final state of transaction should be Opened" in {
    //arrange
    val openedProducerTransaction = createProducerTransaction(Opened, ts)
    val updatedProducerTransaction1 = createProducerTransaction(Updated, ts + 1)
    val updatedProducerTransaction2 = createProducerTransaction(Updated, ts + 2)

    //act
    val finalState = transactionStateHandler.transitProducerTransactionToNewState(
      Seq(openedProducerTransaction,
        updatedProducerTransaction1,
        updatedProducerTransaction2
      ))

    //assert
    finalState.state shouldBe Opened
  }

  private def createProducerTransaction(transactionState: TransactionStates, ts: Long) = {
    val producerTransaction = com.bwsw.tstreamstransactionserver.rpc.ProducerTransaction(streamName, streamPartitions, ts, transactionState, quantity, openedTTL)

    ProducerTransactionRecord(producerTransaction, streamNameToLong, ts)
  }
}