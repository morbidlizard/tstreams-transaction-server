package ut

import java.util.concurrent.TimeUnit

import com.bwsw.tstreamstransactionserver.netty.server.db.zk.StreamDatabaseZK
import com.bwsw.tstreamstransactionserver.netty.server.streamService.{StreamKey, StreamRecord, StreamValue}
import com.bwsw.tstreamstransactionserver.netty.server.subscriber.{SubscribersObserver, StreamPartitionUnit}
import org.apache.curator.framework.CuratorFramework
import org.apache.zookeeper.CreateMode
import org.scalatest.{FlatSpec, Matchers}
import util.Utils

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class SubscribersObserverTest
  extends FlatSpec
    with Matchers
{

  private def putSubscriberInStream(client: CuratorFramework,
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

  private def deleteSubscriberInStream(client: CuratorFramework,
                                       path: String,
                                       partition: Int,
                                       subscriber: String
                                      ): Unit = {
    client.delete()
      .forPath(
        s"$path/subscribers/$partition/$subscriber"
      )
  }

  "Subscribers observer" should "throws exception if it is shutdown more than once" in {
    val (zkServer, zkClient) = Utils.startZkServerAndGetIt
    val streamDatabaseZK = new StreamDatabaseZK(zkClient, "/tts")
    val timeToUpdateMs = 200

    val observer = new SubscribersObserver(
      zkClient,
      streamDatabaseZK,
      timeToUpdateMs
    )

    observer.shutdown()
    zkClient.close()
    zkServer.close()

    assertThrows[IllegalStateException](
      observer.shutdown()
    )
  }




  it should "return none subscribers" in {
    val (zkServer, zkClient) = Utils.startZkServerAndGetIt
    val streamDatabaseZK = new StreamDatabaseZK(zkClient, "/tts")
    val timeToUpdateMs = 200

    val observer = new SubscribersObserver(
      zkClient,
      streamDatabaseZK,
      timeToUpdateMs
    )

    val rand = scala.util.Random
    val streamKeys  = new ArrayBuffer[StreamKey]()
    (0 to 10).foreach{index =>
      val streamBody = StreamValue(index.toString, 100, None, 1000L, None)
      streamKeys += streamDatabaseZK.putStream(streamBody)

      observer.addSteamPartition(index, rand.nextInt(100))
    }

    TimeUnit.MILLISECONDS.sleep(timeToUpdateMs)

    streamKeys foreach {key =>
      observer.getStreamPartitionSubscribers(key.id, 1) shouldBe None
    }

    observer.shutdown()
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

    val observer = new SubscribersObserver(
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
          putSubscriberInStream(zkClient, pathToStream, partition, subscriber.toString)
        }
        streamPartitions.get(streamID) match {
          case None =>
            streamPartitions.put(streamID, ArrayBuffer.empty[Int])
          case Some(acc) =>
            acc += partition
        }
        observer.addSteamPartition(streamID, partition)
      }
    }

    TimeUnit.MILLISECONDS.sleep(timeToUpdateMs)

    streamKeys foreach {key =>
      streamPartitions(key.id).foreach{partition =>
        val subscriberNumber = observer
          .getStreamPartitionSubscribers(key.id, partition)
          .get.size
        subscriberNumber shouldBe streamPartitionSubscribersNumber(key.id, partition)
      }
    }

    observer.shutdown()
    zkClient.close()
    zkServer.close()
  }


  it should "return a subscriber before removing it on zk path and return None after removing the subscriber" in {
    val (zkServer, zkClient) = Utils.startZkServerAndGetIt
    val streamDatabaseZK = new StreamDatabaseZK(zkClient, "/tts")
    val timeToUpdateMs = 200

    val observer = new SubscribersObserver(
      zkClient,
      streamDatabaseZK,
      timeToUpdateMs
    )

    val streamBody = StreamValue(0.toString, 100, None, 1000L, None)
    val streamKey   = streamDatabaseZK.putStream(streamBody)
    val streamRecord = streamDatabaseZK.getStream(streamKey).get
    val partition  = 1

    observer.addSteamPartition(streamKey.id, partition)
    TimeUnit.MILLISECONDS.sleep(timeToUpdateMs)

    observer
      .getStreamPartitionSubscribers(streamKey.id, partition) shouldBe None

    putSubscriberInStream(zkClient, streamRecord.zkPath, partition, "test")
    observer.addSteamPartition(streamKey.id, partition)
    TimeUnit.MILLISECONDS.sleep(timeToUpdateMs)

    observer
      .getStreamPartitionSubscribers(streamKey.id, partition)
      .get.get(0) shouldBe "test"

    deleteSubscriberInStream(zkClient, streamRecord.zkPath, partition, "test")
    TimeUnit.MILLISECONDS.sleep(timeToUpdateMs)

    observer
      .getStreamPartitionSubscribers(streamKey.id, partition) shouldBe None


    observer.shutdown()
    zkClient.close()
    zkServer.close()
  }


}
