package com.bwsw.tstreamstransactionserver.netty.client

import java.io.{Closeable, File}
import java.util.concurrent.TimeUnit

import com.bwsw.tstreamstransactionserver.exception.Throwable.{MasterDataIsIllegalException, MasterIsPersistentZnodeException, MasterPathIsAbsent, ZkNoConnectionException}
import com.bwsw.tstreamstransactionserver.netty.SocketHostPortPair
import com.bwsw.tstreamstransactionserver.netty.client.ZKLeaderClientToGetMaster._
import org.apache.curator.RetryPolicy
import org.apache.curator.framework.recipes.cache.{ChildData, NodeCache, NodeCacheListener}
import org.apache.curator.framework.state.ConnectionStateListener
import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import org.slf4j.LoggerFactory


class ZKLeaderClientToGetMaster(connection: CuratorFramework,
                                prefix: String,
                                isConnectionCloseable: Boolean,
                                connectionStateListener: ConnectionStateListener
                               )
  extends NodeCacheListener with Closeable {

  private val logger = LoggerFactory.getLogger(this.getClass)
  @volatile private[client] var master: Either[Throwable, Option[SocketHostPortPair]] = Right(None)

  def this(endpoints: String,
           sessionTimeoutMillis: Int,
           connectionTimeoutMillis: Int,
           policy: RetryPolicy,
           prefix: String,
           isConnectionCloseable: Boolean,
           connectionStateListener: ConnectionStateListener) = {
    this({
      val connection = CuratorFrameworkFactory.builder()
        .sessionTimeoutMs(sessionTimeoutMillis)
        .connectionTimeoutMs(connectionTimeoutMillis)
        .retryPolicy(policy)
        .connectString(endpoints)
        .build()

      connection.start()
      val isConnected = connection.blockUntilConnected(connectionTimeoutMillis, TimeUnit.MILLISECONDS)
      if (isConnected) connection else throw new ZkNoConnectionException(endpoints)
    },
      prefix,
      isConnectionCloseable,
      connectionStateListener
    )
  }

  connection.getConnectionStateListenable.addListener(connectionStateListener)

  private val nodeToWatch = new NodeCache(connection, prefix, false)
  nodeToWatch.getListenable.addListener(this)

  def start(): Unit = nodeToWatch.start()


  private def checkOnPathToMasterDoesExist() = {
    val pathToMaster = new File(prefix).getParent
    val isExist =
      Option(connection.checkExists().forPath(pathToMaster)).isDefined
    if (!isExist)
      throw new MasterPathIsAbsent(pathToMaster)
  }
  checkOnPathToMasterDoesExist()

  override def close(): Unit = {
    nodeToWatch.close()
    if (isConnectionCloseable)
      connection.close()
  }

  override def nodeChanged(): Unit = {
    Option(nodeToWatch.getCurrentData) match {
      case Some(node) =>
        if (node.getStat.getEphemeralOwner == nonEphemeralNode)
          master = Left(new MasterIsPersistentZnodeException(node.getPath))
        else
          master = setMasterIfCandidateExists(node)
      case None =>
        scala.util.Try(checkOnPathToMasterDoesExist()) match {
          case scala.util.Success(_) =>
            master = Right(None)
          case scala.util.Failure(throwable) =>
            master = Left(throwable)
        }
    }
  }

  private def setMasterIfCandidateExists(node: ChildData) = {
    val hostPort = new String(node.getData)
    val connectionData = connection.getZookeeperClient.getCurrentConnectionString

    SocketHostPortPair.fromString(hostPort) match {
      case Some(hostPortPairOpt) =>
        Right(Some(hostPortPairOpt))
      case None =>
        logger.error(s"Master information data ($hostPort) is corrupted for $connectionData$prefix.")
        Left(new MasterDataIsIllegalException(node.getPath, hostPort))
    }
  }
}

object ZKLeaderClientToGetMaster{
  val nonEphemeralNode = 0L
}


