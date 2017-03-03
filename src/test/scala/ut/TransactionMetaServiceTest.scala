package ut

import com.bwsw.tstreamstransactionserver.netty.server.transactionMetadataService.{Key, ProducerTransaction, ProducerTransactionKey}
import org.scalatest.{FlatSpec, Matchers}
import transactionService.rpc.TransactionStates

class TransactionMetaServiceTest extends FlatSpec with Matchers {

  "Key" should "be serialized/deserialized" in {
    val key = Key(1L, 10, 15L)
    Key.entryToObject(key.toDatabaseEntry) shouldBe key
  }

  it should "be serialized/deserialized with negative stream" in {
    val key = Key(-1L, 10, 15L)
    Key.entryToObject(key.toDatabaseEntry) shouldBe key
  }

  it should "be serialized/deserialized with negative partition" in {
    val key = Key(1L, -10, 15L)
    Key.entryToObject(key.toDatabaseEntry) shouldBe key
  }

  it should "be serialized/deserialized with negative transaction" in {
    val key = Key(1L, 10, -15L)
    Key.entryToObject(key.toDatabaseEntry) shouldBe key
  }

  "ProducerTransaction" should "be serialized/deserialized" in {
    val producerTransaction = ProducerTransaction(TransactionStates.Opened, 10, Long.MaxValue)
    ProducerTransaction.entryToObject(producerTransaction.toDatabaseEntry) shouldBe producerTransaction
  }

  it should "be serialized/deserialized with negative quantity" in {
    val producerTransaction = ProducerTransaction(TransactionStates.Opened, -10, Long.MaxValue)
    ProducerTransaction.entryToObject(producerTransaction.toDatabaseEntry) shouldBe producerTransaction
  }

  it should "be serialized/deserialized with negative ttl" in {
    val producerTransaction = ProducerTransaction(TransactionStates.Opened, -10, Long.MinValue)
    ProducerTransaction.entryToObject(producerTransaction.toDatabaseEntry) shouldBe producerTransaction
  }
}
