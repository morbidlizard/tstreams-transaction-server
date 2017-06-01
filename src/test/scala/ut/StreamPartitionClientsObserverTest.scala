package ut

import java.util.concurrent.TimeUnit

import com.bwsw.tstreamstransactionserver.netty.server.db.zk.StreamDatabaseZK
import com.bwsw.tstreamstransactionserver.netty.server.streamService.{StreamKey, StreamRecord, StreamValue}
import com.bwsw.tstreamstransactionserver.netty.server.subscriber.{StreamPartitionClientsObserver, StreamPartitionUnit}
import org.apache.curator.framework.CuratorFramework
import org.apache.zookeeper.CreateMode
import org.scalatest.{FlatSpec, Matchers}
import util.Utils

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class StreamPartitionClientsObserverTest
  extends FlatSpec
    with Matchers
{

  private def putSubscriberOnStream(client: CuratorFramework,
                                    path: String,
                                    partition: Int,
                                    subscriber: String
                           ): Unit = {
    client.create()
      .creatingParentsIfNeeded()
      .withMode(CreateMode.PERSISTENT)
      .forPath(
        s"$path/subscribers/$partition/$subscriber",
        Array.emptyByteArray
      )
  }

  private def deleteSubscriberOnStream(client: CuratorFramework,
                                       path: String,
                                       partition: Int,
                                       subscriber: String
                                      ): Unit = {
    client.delete()
      .forPath(
        s"$path/subscribers/$partition/$subscriber"
      )
  }


  "StreamPartitionClientsObserver" should "return none subscribers" in {
    val (zkServer, zkClient) = Utils.startZkServerAndGetIt
    val streamDatabaseZK = new StreamDatabaseZK(zkClient, "/tts")
    val timeToUpdateMs = 200

    val observer = new StreamPartitionClientsObserver(
      zkClient,
      streamDatabaseZK,
      timeToUpdateMs
    )

    val rand = scala.util.Random
    val streamKeys  = new ArrayBuffer[StreamKey]()
    (0 to 10).foreach{index =>
      val streamBody = StreamValue(index.toString, 100, None, 1000L, None)
      streamKeys += streamDatabaseZK.putStream(streamBody)

      observer.addSteamPartition(StreamPartitionUnit(index, rand.nextInt(100)))
    }

    TimeUnit.MILLISECONDS.sleep(timeToUpdateMs)

    streamKeys foreach {key =>
      observer.getStreamPartitionSubscribers(StreamPartitionUnit(key.id, 1)) shouldBe None
    }

    zkClient.close()
    zkServer.close()
  }

  it should "return all subscribers" in {
    val (zkServer, zkClient) = Utils.startZkServerAndGetIt
    val streamDatabaseZK = new StreamDatabaseZK(zkClient, "/tts")
    val timeToUpdateMs = 200

    val maxStreams = 10
    val partitionMax = 7
    val subscriberMax = 5

    val observer = new StreamPartitionClientsObserver(
      zkClient,
      streamDatabaseZK,
      timeToUpdateMs
    )

    val rand = scala.util.Random
    val streamKeys  = new ArrayBuffer[StreamKey]()

    val streamPartitions = mutable.Map[Int, ArrayBuffer[Int]]()
    val streamPartitionSubscribersNumber = mutable.Map[(Int, Int), Int]()
    (0 to maxStreams).foreach{streamID =>
      val streamBody = StreamValue(streamID.toString, partitionMax, None, 1000L, None)
      val streamKey  = streamDatabaseZK.putStream(streamBody)
      streamKeys += streamKey

      val streamRecord = streamDatabaseZK.getStream(streamKey)
      val pathToStream = streamRecord.get.zkPath

      (0 to rand.nextInt(partitionMax)) foreach { partition =>
        val subscriberNumber = rand.nextInt(subscriberMax)
        streamPartitionSubscribersNumber.put((streamID, partition), subscriberNumber + 1)
        (0 to subscriberNumber) foreach { subscriber =>
          putSubscriberOnStream(zkClient, pathToStream, partition, subscriber.toString)
        }
        streamPartitions.get(streamID) match {
          case None =>
            streamPartitions.put(streamID, ArrayBuffer.empty[Int])
          case Some(acc) =>
            acc += partition
        }
        observer.addSteamPartition(StreamPartitionUnit(streamID, partition))
      }
    }

    TimeUnit.MILLISECONDS.sleep(timeToUpdateMs)

    streamKeys foreach {key =>
      streamPartitions(key.id).foreach{partition =>
        val subscriberNumber = observer
          .getStreamPartitionSubscribers(StreamPartitionUnit(key.id, partition))
          .get.size
        subscriberNumber shouldBe streamPartitionSubscribersNumber(key.id, partition)
      }
    }

    zkClient.close()
    zkServer.close()
  }


  it should "return a subscriber before removing it on zk path and return None after removing the subscriber" in {
    val (zkServer, zkClient) = Utils.startZkServerAndGetIt
    val streamDatabaseZK = new StreamDatabaseZK(zkClient, "/tts")
    val timeToUpdateMs = 200

    val observer = new StreamPartitionClientsObserver(
      zkClient,
      streamDatabaseZK,
      timeToUpdateMs
    )

    val streamBody = StreamValue(0.toString, 100, None, 1000L, None)
    val streamKey   = streamDatabaseZK.putStream(streamBody)
    val streamRecord = streamDatabaseZK.getStream(streamKey).get
    val partition  = 1

    observer.addSteamPartition(StreamPartitionUnit(streamKey.id, partition))
    TimeUnit.MILLISECONDS.sleep(timeToUpdateMs)

    observer
      .getStreamPartitionSubscribers(StreamPartitionUnit(streamKey.id, partition)) shouldBe None

    putSubscriberOnStream(zkClient, streamRecord.zkPath, partition, "test")
    observer.addSteamPartition(StreamPartitionUnit(streamKey.id, partition))
    TimeUnit.MILLISECONDS.sleep(timeToUpdateMs)

    observer
      .getStreamPartitionSubscribers(StreamPartitionUnit(streamKey.id, partition))
      .get.get(0) shouldBe "test"

    deleteSubscriberOnStream(zkClient, streamRecord.zkPath, partition, "test")
    TimeUnit.MILLISECONDS.sleep(timeToUpdateMs)

    observer
      .getStreamPartitionSubscribers(StreamPartitionUnit(streamKey.id, partition)) shouldBe None


    zkClient.close()
    zkServer.close()
  }


}
