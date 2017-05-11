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

    val streamKey    = zkDatabase.putStream(streamValue.name, streamValue.partitions, streamValue.description, streamValue.ttl)
    val streamRecord = zkDatabase.getStream(streamKey)

    streamRecord shouldBe defined
    val streamObj = streamRecord.get

    streamObj.key shouldBe streamKey
    streamObj.stream shouldBe streamValue

    zkClient.close()
    zkServer.close()
  }

  it should "put stream, delete it, then on calling getStream return None" in {
    val (zkServer, zkClient) = startZkServerAndGetIt

    val zkDatabase = new StreamDatabaseZK(zkClient, path)

    val streamValue  = getStreamValue

    val streamKey    = zkDatabase.putStream(streamValue.name, streamValue.partitions, streamValue.description, streamValue.ttl)
    zkDatabase.delStream(streamKey) shouldBe true
    val streamRecord = zkDatabase.getStream(streamKey)

    streamRecord should not be defined

    zkClient.close()
    zkServer.close()
  }


}
