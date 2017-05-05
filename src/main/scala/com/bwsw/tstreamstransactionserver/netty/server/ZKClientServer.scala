package com.bwsw.tstreamstransactionserver.netty.server

import java.io.Closeable
import java.util
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong

import com.bwsw.tstreamstransactionserver.exception.Throwable.{InvalidSocketAddress, ZkNoConnectionException}
import com.bwsw.tstreamstransactionserver.netty.InetSocketAddressClass
import com.bwsw.tstreamstransactionserver.netty.server.streamService.{StreamCache, StreamKey, StreamRecord, StreamValue}
import com.bwsw.tstreamstransactionserver.rpc
import com.google.common.net.InetAddresses
import io.netty.resolver.dns.DnsNameResolver
import org.apache.curator.RetryPolicy
import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import org.apache.curator.framework.recipes.atomic.DistributedAtomicLong
import org.apache.curator.framework.recipes.leader.Participant
import org.apache.zookeeper.CreateMode
import org.apache.zookeeper.ZooDefs.{Ids, Perms}
import org.apache.zookeeper.data.ACL
import org.slf4j.LoggerFactory

import scala.util.Try

class ZKClientServer(serverAddress: String,
                     serverPort: Int,
                     endpoints: String,
                     sessionTimeoutMillis: Int,
                     connectionTimeoutMillis: Int,
                     policy: RetryPolicy
                    ) extends Closeable
{

  private val logger = LoggerFactory.getLogger(this.getClass)

  private val socketAddress: String =
    if (InetSocketAddressClass.isValidSocketAddress(serverAddress, serverPort)) s"$serverAddress:$serverPort"
    else throw new InvalidSocketAddress(s"Invalid socket address $serverAddress:$serverPort")

  private val client = {
    val connection = CuratorFrameworkFactory.builder()
      .sessionTimeoutMs(sessionTimeoutMillis)
      .connectionTimeoutMs(connectionTimeoutMillis)
      .retryPolicy(policy)
      .connectString(endpoints)
      .build()

    connection.start()
    val isConnected = connection.blockUntilConnected(connectionTimeoutMillis, TimeUnit.MILLISECONDS)
    if (isConnected) connection else throw new ZkNoConnectionException(endpoints)
  }

  final def fileIDGenerator(path: String, initValue: Long) = new FileIDGenerator(path, initValue)
  final class FileIDGenerator(path: String, initValue: Long) {
    private val distributedAtomicLong = new DistributedAtomicLong(client, path, policy)
    private val atomicLong = new AtomicLong(initValue)
    distributedAtomicLong.forceSet(initValue)
   // if (!distributedAtomicLong.initialize(initValue)) throw new Exception(s"Can't initialize counter by value $initValue.")

    def current: Long = atomicLong.get()

    def increment: Long = {
      val operation = distributedAtomicLong.increment()
      if (operation.succeeded()) {
        val newID = operation.postValue()
        atomicLong.set(newID)
        newID
      }
      else throw new Exception(s"Can't increment counter by 1: previous was ${operation.preValue()} but now it's ${operation.postValue()} ")
    }
  }

  final class StreamDatabaseZK(path: String) extends StreamCache {
    import scala.collection.JavaConverters._
    private val streamCache = new java.util.concurrent.ConcurrentHashMap[StreamKey, StreamValue]()

    private val seqPrefix = "id"
    private val streamsPath = s"$path/$seqPrefix"
    private def buildPath(streamID: Int): String = f"$streamsPath$streamID%010d"
    private def getIDFromPath(pathWithId: String): String = pathWithId.splitAt(path.length + seqPrefix.length + 1)._2
    private def getID(id: String): String = id.splitAt(seqPrefix.length + 1)._2

    private def initializeCache(): Unit = {
      val statOpt = Option(client.checkExists().creatingParentContainersIfNeeded().forPath(path))
      val streams = statOpt.map{_ =>
        val streamsIds = client.getChildren.forPath(path).asScala
        streamsIds.map{id =>
          val streamValueBinary = client.getData.forPath(path + '/' + id)
          val streamKey   = StreamKey(getID(id).toInt)
          val streamValue = StreamValue.fromByteArray(streamValueBinary)
          if (logger.isDebugEnabled)
            logger.debug(s"Loading stream to cache: " +
              s"id ${streamKey.id}, " +
              s"name ${streamValue.name}, " +
              s"partitions ${streamValue.partitions}, " +
              s"description ${streamValue.description}" +
              s"ttl ${streamValue.ttl}"
            )
          StreamRecord(streamKey, streamValue)
        }
      }.getOrElse{
        if (logger.isDebugEnabled)
          logger.debug(s"There are no streams on path $path, may be it's first run of TTS.")
        Seq.empty[StreamRecord]
      }

      streams.foreach(record => streamCache.put(record.key, record.stream))
    }

    override def putStream(stream: String, partitions: Int, description: Option[String], ttl: Long): StreamKey = {
      val streamValue = StreamValue(stream, partitions, description, ttl)

      val id = client.create()
        .creatingParentsIfNeeded()
        .withMode(CreateMode.PERSISTENT_SEQUENTIAL)
        .forPath(streamsPath, streamValue.toByteArray)
      val streamKey = StreamKey(getIDFromPath(id).toInt)
      if (logger.isDebugEnabled)
        logger.debug(s"Persisted stream: " +
          s"id ${streamKey.id}, " +
          s"name ${streamValue.name}, " +
          s"partitions ${streamValue.partitions}, " +
          s"description ${streamValue.description}" +
          s"ttl ${streamValue.ttl}"
        )
      StreamRecord(streamKey, streamValue)
      streamCache.put(streamKey, streamValue)
      streamKey
    }

    override def checkStreamExists(streamID: Int): Boolean = {
      val streamKey = StreamKey(streamID)
      val doesExist = Option(streamCache.get(streamKey))
        .orElse(Option(client.checkExists().forPath(buildPath(streamID))))
        .exists(_ => true)
      if (logger.isDebugEnabled)
        logger.debug(s"Stream with id $streamID ${if (doesExist) "exists" else "doesn't exist"}")
      doesExist
    }

    override def delStream(streamID: Int): Boolean = {
      if (checkStreamExists(streamID)) {
        client.delete().forPath(buildPath(streamID))
        streamCache.remove( StreamKey(streamID))
        if (logger.isDebugEnabled)
          logger.debug(s"Stream with id $streamID is deleted")
        true
      }
      else false
    }

    override def getStream(streamID: Int): Option[StreamRecord] = {
      val streamKey = StreamKey(streamID)
      Option(streamCache.get(streamKey))
        .map(streamValue => StreamRecord(streamKey, streamValue))
        .orElse {
          val streamValueOpt = Option(client.getData.forPath(buildPath(streamID)))
          streamValueOpt.map { streamValueBinary =>
            val streamValue = StreamValue.fromByteArray(streamValueBinary)
            streamCache.put(streamKey, streamValue)
            StreamRecord(streamKey, streamValue)
          }
        }
    }

    override def getAllStreams: Seq[StreamRecord] = {
      streamCache.asScala.map{case (key,value) => StreamRecord(key,value)}.toSeq
    }
    initializeCache()
  }

  def streamDatabase(path: String) = new StreamDatabaseZK(path)


//  final def replicationGroup(path: String) = new ReplicationGroup(path)
//  final class ReplicationGroup(path: String) {
//    import org.apache.curator.framework.recipes.cache.{PathChildrenCache, PathChildrenCacheListener}
//    import org.apache.curator.framework.recipes.leader.{LeaderLatch, LeaderLatchListener}
//    import scala.collection.JavaConverters._
//
//    final class Listener(listener: PathChildrenCacheListener) {
//      private val cache = new PathChildrenCache(client, path, true)
//      cache.getListenable.addListener(listener)
//      cache.start()
//
//      def close(): Unit = cache.close()
//    }
//
//    final class Election(listener: LeaderLatchListener) {
//      private val leaderLatch = new LeaderLatch(client, path, socketAddress, LeaderLatch.CloseMode.NOTIFY_LEADER)
//      leaderLatch.addListener(listener)
//
//      def participants: Iterable[Participant] = leaderLatch.getParticipants.asScala
//
//      def join(): Unit = leaderLatch.start()
//      def leave(): Unit = leaderLatch.close(LeaderLatch.CloseMode.NOTIFY_LEADER)
//    }
//
//    def election(listener: LeaderLatchListener) = new Election(listener)
//    def listener(listener: PathChildrenCacheListener) = new Listener(listener)
//  }


  final def putSocketAddress(path: String): Try[String] = {
    scala.util.Try(client.delete().deletingChildrenIfNeeded().forPath(path))
    scala.util.Try{
      val permissions = new util.ArrayList[ACL]()
      permissions.add(new ACL(Perms.READ, Ids.ANYONE_ID_UNSAFE))

      client.create().creatingParentsIfNeeded()
        .withMode(CreateMode.EPHEMERAL)
        .withACL(permissions)
        .forPath(path, socketAddress.getBytes())
    }
  }

  override def close(): Unit = scala.util.Try(client.close())
}