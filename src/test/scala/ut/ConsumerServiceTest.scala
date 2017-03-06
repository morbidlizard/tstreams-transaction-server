package ut

import com.bwsw.tstreamstransactionserver.netty.server.consumerService.{ConsumerTransaction, Key}
import org.scalatest.{FlatSpec, Matchers}

class ConsumerServiceTest extends FlatSpec with Matchers {
  "Key" should "be serialized/deserialized" in {
    val key = Key("testCheckpoint", 1L, 5)
    Key.entryToObject(key.toDatabaseEntry) shouldBe key
  }

  it should "be serialized/deserialized with negative stream" in {
    val key = Key("testCheckpoint", -1L, 5)
    Key.entryToObject(key.toDatabaseEntry) shouldBe key
  }

  it should "be serialized/deserialized with negative partition" in {
    val key = Key("testCheckpoint", -1L, -5)
    Key.entryToObject(key.toDatabaseEntry) shouldBe key
  }

  "ConsumerTransaction" should "be serialized/deserialized" in {
    val consumerTranasction = ConsumerTransaction(1L)
    ConsumerTransaction.entryToObject(consumerTranasction.toDatabaseEntry) shouldBe consumerTranasction
  }

  it should "be serialized/deserialized with negative transaction" in {
    val consumerTranasction = ConsumerTransaction(1L)
    ConsumerTransaction.entryToObject(consumerTranasction.toDatabaseEntry) shouldBe consumerTranasction
  }

}
