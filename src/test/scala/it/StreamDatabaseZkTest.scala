package it

import java.util.concurrent.TimeUnit

import com.bwsw.tstreamstransactionserver.netty.server.db.zk.StreamDatabaseZK
import com.bwsw.tstreamstransactionserver.netty.server.streamService.StreamValue
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.retry.RetryNTimes
import org.apache.curator.test.TestingServer
import org.scalatest.{BeforeAndAfterEach, FlatSpec, Matchers}

class StreamDatabaseZkTest extends FlatSpec with Matchers with BeforeAndAfterEach {
  private val sessionTimeoutMillis = 1000
  private val connectionTimeoutMillis = 1000

  private def startZkServerAndGetIt = {
    val zkServer = new TestingServer(true)

    val zkClient = CuratorFrameworkFactory.builder
      .sessionTimeoutMs(sessionTimeoutMillis)
      .connectionTimeoutMs(connectionTimeoutMillis)
      .retryPolicy(new RetryNTimes(3, 100))
      .connectString(zkServer.getConnectString)
      .build()

    zkClient.start()
    zkClient.blockUntilConnected(100, TimeUnit.MILLISECONDS)

    (zkServer, zkClient)
  }

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


    val streamRecord = zkDatabase.getStream(streamValue.name)

    streamRecord shouldBe defined
    streamRecord.get shouldBe newStream

    zkClient.close()
    zkServer.close()
  }

}
