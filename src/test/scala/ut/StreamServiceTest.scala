package ut

import com.bwsw.tstreamstransactionserver.netty.server.streamService.{StreamKey, StreamValue}
import org.scalatest.{FlatSpec, Matchers}

class StreamServiceTest extends FlatSpec with Matchers {

  "Key" should "be serialized/deserialized" in {
    val key = StreamKey(1L)
    StreamKey.entryToObject(key.toDatabaseEntry) shouldBe key
    StreamKey.fromByteArray(key.toByteArray) shouldBe key
  }

  it should "be serialized/deserialized with negative transaction" in {
    val key = StreamKey(-1L)
    StreamKey.entryToObject(key.toDatabaseEntry) shouldBe key
    StreamKey.fromByteArray(key.toByteArray) shouldBe key
  }

  "StreamWithoutKey" should "be serialized/deserialized without description" in {
    val stream = StreamValue("streamNumber1", 10, None, Long.MaxValue, System.currentTimeMillis(), deleted = false)
    StreamValue.entryToObject(stream.toDatabaseEntry) shouldBe stream
    StreamValue.fromByteArray(stream.toByteArray) shouldBe stream
  }

  it should "be serialized/deserialized with negative partitions" in {
    val stream = StreamValue("streamNumber1", -5, None, Long.MaxValue, System.currentTimeMillis(), deleted = false)
    StreamValue.entryToObject(stream.toDatabaseEntry) shouldBe stream
    StreamValue.fromByteArray(stream.toByteArray) shouldBe stream
  }

  it should "be serialized/deserialized with negative ttl" in {
    val stream = StreamValue("streamNumber1", -5, None, Long.MinValue, System.currentTimeMillis(), deleted = false)
    StreamValue.entryToObject(stream.toDatabaseEntry) shouldBe stream
    StreamValue.fromByteArray(stream.toByteArray) shouldBe stream
  }

  it should "be serialized/deserialized with description" in {
    val stream = StreamValue("streamNumber1", 70, Some("test"), Long.MaxValue, System.currentTimeMillis(), deleted = false)
    StreamValue.entryToObject(stream.toDatabaseEntry) shouldBe stream
    StreamValue.fromByteArray(stream.toByteArray) shouldBe stream
  }

  it should "be serialized/deserialized with empty description" in {
    val stream = StreamValue("streamNumber1", 16, Some(""), Long.MaxValue, System.currentTimeMillis(), deleted = true)
    StreamValue.entryToObject(stream.toDatabaseEntry) shouldBe stream
    StreamValue.fromByteArray(stream.toByteArray) shouldBe stream
  }
}
