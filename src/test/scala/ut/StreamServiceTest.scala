package ut

import com.bwsw.tstreamstransactionserver.netty.server.streamService.{Key, Stream}
import org.scalatest.{FlatSpec, Matchers}

class StreamServiceTest extends FlatSpec with Matchers {

  "Key" should "be serialized/deserialized" in {
    val key = Key(1L)
    Key.entryToObject(key.toDatabaseEntry) shouldBe key
  }

  it should "be serialized/deserialized with negative transaction" in {
    val key = Key(-1L)
    Key.entryToObject(key.toDatabaseEntry) shouldBe key
  }

  "Stream" should "be serialized/deserialized without description" in {
    val stream = Stream("streamNumber1", 10, None, Long.MaxValue)
    Stream.entryToObject(stream.toDatabaseEntry) shouldBe stream
  }

  it should "be serialized/deserialized with negative partitions" in {
    val stream = Stream("streamNumber1", -5, None, Long.MaxValue)
    Stream.entryToObject(stream.toDatabaseEntry) shouldBe stream
  }

  it should "be serialized/deserialized with negative ttl" in {
    val stream = Stream("streamNumber1", -5, None, Long.MinValue)
    Stream.entryToObject(stream.toDatabaseEntry) shouldBe stream
  }

  it should "be serialized/deserialized with description" in {
    val stream = Stream("streamNumber1", 70, Some("test"), Long.MaxValue)
    Stream.entryToObject(stream.toDatabaseEntry) shouldBe stream
  }

  it should "be serialized/deserialized with empty description" in {
    val stream = Stream("streamNumber1", 16, Some(""), Long.MaxValue)
    Stream.entryToObject(stream.toDatabaseEntry) shouldBe stream
  }
}
