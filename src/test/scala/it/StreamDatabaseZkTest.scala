package it

import com.bwsw.tstreamstransactionserver.exception.Throwable.StreamOverwriteProhibited
import com.bwsw.tstreamstransactionserver.netty.server.db.zk.StreamDatabaseZK
import com.bwsw.tstreamstransactionserver.netty.server.streamService.{StreamRecord, StreamValue}
import org.scalatest.{BeforeAndAfterEach, FlatSpec, Matchers}
import Utils._

class StreamDatabaseZkTest extends FlatSpec with Matchers with BeforeAndAfterEach {
  private def getStreamValue = StreamValue("test_stream", 20, None, 5)

  private val path = "/tts/test_path"

  "One" should "put stream and get it back" in {
    val (zkServer, zkClient) = startZkServerAndGetIt

    val zkDatabase = new StreamDatabaseZK(zkClient, path)

    val streamValue  = getStreamValue

    val streamKey    = zkDatabase.putStream(streamValue)
    val streamRecordByName = zkDatabase.getStream(streamValue.name)
    val streamRecordByID = zkDatabase.getStream(streamKey)

    streamRecordByID shouldBe defined
    val streamObj = streamRecordByID.get

    streamObj.key shouldBe streamKey
    streamObj.stream shouldBe streamValue
    streamObj.stream shouldBe streamRecordByName.get

    zkClient.close()
    zkServer.close()
  }

  it should "put stream, try to put new stream with the same and got exception" in {
    val (zkServer, zkClient) = startZkServerAndGetIt

    val zkDatabase = new StreamDatabaseZK(zkClient, path)

    val streamValue = getStreamValue
    zkDatabase.putStream(streamValue)

    val newStream = StreamValue("test_stream", 100, Some("overwrite"), 10)

    assertThrows[StreamOverwriteProhibited] {
      zkDatabase.putStream(newStream)
    }

    zkDatabase.checkStreamExists(newStream.name) shouldBe true

    zkClient.close()
    zkServer.close()
  }

  it should "put stream, delete it, then the one calls getStream and it returns None" in {
    val (zkServer, zkClient) = startZkServerAndGetIt

    val zkDatabase = new StreamDatabaseZK(zkClient, path)

    val streamValue  = getStreamValue

    zkDatabase.putStream(streamValue)
    zkDatabase.delStream(streamValue.name) shouldBe true
    val streamRecord = zkDatabase.getStream(streamValue.name)

    streamRecord should not be defined

    zkClient.close()
    zkServer.close()
  }

  it should "put stream, delete it, then on checking stream the one see stream doesn't exist" in {
    val (zkServer, zkClient) = startZkServerAndGetIt

    val zkDatabase = new StreamDatabaseZK(zkClient, path)

    val streamValue  = getStreamValue

    zkDatabase.putStream(streamValue)
    zkDatabase.delStream(streamValue.name) shouldBe true
    val streamRecord = zkDatabase.checkStreamExists(streamValue.name)

    streamRecord shouldBe false

    zkClient.close()
    zkServer.close()
  }

  it should "put stream, delete it, then put a new stream with same name a get it back" in {
    val (zkServer, zkClient) = startZkServerAndGetIt

    val zkDatabase = new StreamDatabaseZK(zkClient, path)

    val streamValue = getStreamValue
    zkDatabase.putStream(streamValue)
    zkDatabase.delStream(streamValue.name) shouldBe true

    val newStream = StreamValue("test_stream", 100, Some("overwrite"), 10)
    zkDatabase.putStream(newStream)

    zkDatabase.checkStreamExists(newStream.name) shouldBe true

    val streamRecord = zkDatabase.getStream(newStream.name)
    streamRecord shouldBe defined
    streamRecord.get shouldBe newStream

    zkClient.close()
    zkServer.close()
  }

  it should "put stream, delete it, then get it by ID and see there the stream record" in {
    val (zkServer, zkClient) = startZkServerAndGetIt

    val zkDatabase = new StreamDatabaseZK(zkClient, path)

    val streamValue = getStreamValue
    val streamKey = zkDatabase.putStream(streamValue)

    zkDatabase.delStream(streamValue.name) shouldBe true
    zkDatabase.checkStreamExists(streamValue.name) shouldBe false

    val retrievedStream = zkDatabase.getStream(streamKey)

    retrievedStream shouldBe defined

    retrievedStream.get shouldBe StreamRecord(streamKey, streamValue)

    zkClient.close()
    zkServer.close()
  }

  it should "put stream, delete it, then get it by ID and see there the stream record even if stream is overwritten" in {
    val (zkServer, zkClient) = startZkServerAndGetIt

    val zkDatabase = new StreamDatabaseZK(zkClient, path)

    val streamValue = getStreamValue
    val streamKey = zkDatabase.putStream(streamValue)

    zkDatabase.delStream(streamValue.name) shouldBe true
    zkDatabase.checkStreamExists(streamValue.name) shouldBe false

    val newStream = StreamValue("test_stream", 100, Some("overwrite"), 10)
    zkDatabase.putStream(newStream)

    val retrievedStream = zkDatabase.getStream(streamKey)

    retrievedStream shouldBe defined

    retrievedStream.get shouldBe StreamRecord(streamKey, streamValue)

    zkClient.close()
    zkServer.close()
  }

}
