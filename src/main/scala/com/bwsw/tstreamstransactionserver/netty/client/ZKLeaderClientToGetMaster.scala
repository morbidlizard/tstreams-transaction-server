package com.bwsw.tstreamstransactionserver.netty.client

import java.io.{Closeable, File}
import java.util.concurrent.TimeUnit

import com.bwsw.tstreamstransactionserver.exception.Throwable.{MasterDataIsIllegal, MasterIsPersistentZnode, MasterPathIsAbsent, ZkNoConnectionException}
import com.bwsw.tstreamstransactionserver.netty.InetSocketAddressClass
import org.apache.curator.RetryPolicy
import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import org.apache.curator.framework.recipes.cache.{NodeCache, NodeCacheListener}
import org.apache.curator.framework.state.ConnectionStateListener
import org.slf4j.LoggerFactory
import ZKLeaderClientToGetMaster._


class ZKLeaderClientToGetMaster(connection: CuratorFramework,
                                prefix: String,
                                connectionStateListener: ConnectionStateListener
                               )
  extends NodeCacheListener with Closeable {

  private val logger = LoggerFactory.getLogger(this.getClass)
  @volatile private[client] var master: Either[Throwable, Option[InetSocketAddressClass]] = Right(None)

  def this(endpoints: String,
           sessionTimeoutMillis: Int,
           connectionTimeoutMillis: Int,
           policy: RetryPolicy,
           prefix: String,
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
    connection.close()
  }

  override def nodeChanged(): Unit = {
    Option(nodeToWatch.getCurrentData) match {
      case Some(node) =>
        if (node.getStat.getEphemeralOwner == nonEphermalNode) {
          master = Left(new MasterIsPersistentZnode(node.getPath))
        }
        else {
          val addressPort = new String(node.getData)
          val splitIndex = addressPort.lastIndexOf(':')
          if (splitIndex != -1) {
            val (address, port) = addressPort.splitAt(splitIndex)
            val portToInt = scala.util.Try(port.tail.toInt)
            if (portToInt.isSuccess && InetSocketAddressClass.isValidSocketAddress(address, portToInt.get)) {
              master = Right(Some(InetSocketAddressClass(address, portToInt.get)))
            }
            else {
              master = Left(new MasterDataIsIllegal(node.getPath, addressPort))
              if (logger.isDebugEnabled)
                logger.debug(s"" +
                  s"On Zookeeper server(s) ${connection.getZookeeperClient.getCurrentConnectionString} " +
                  s"data(now it is $addressPort) in coordination path $prefix is corrupted!"
                )
            }
          } else {
            master = Right(None)
            if (logger.isDebugEnabled)
              logger.debug(s"" +
                s"On Zookeeper server(s) ${connection.getZookeeperClient.getCurrentConnectionString} " +
                s"data(now it is $addressPort) in coordination path $prefix is corrupted!"
              )
          }
        }
      case None =>
        scala.util.Try(checkOnPathToMasterDoesExist()) match {
          case scala.util.Success(_) =>
            master = Right(None)
          case scala.util.Failure(throwable) =>
            master = Left(throwable)
        }
    }
  }
}

object ZKLeaderClientToGetMaster{
  val nonEphermalNode = 0L
}


