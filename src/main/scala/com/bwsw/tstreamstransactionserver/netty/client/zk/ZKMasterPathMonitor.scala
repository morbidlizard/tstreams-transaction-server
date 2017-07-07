package com.bwsw.tstreamstransactionserver.netty.client.zk

import java.io.File

import com.bwsw.tstreamstransactionserver.exception.Throwable.{MasterDataIsIllegalException, MasterIsPersistentZnodeException, MasterPathIsAbsent}
import com.bwsw.tstreamstransactionserver.netty.SocketHostPortPair
import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.recipes.cache.{ChildData, NodeCache, NodeCacheListener}
import org.slf4j.LoggerFactory

private object ZKMasterPathMonitor{
  val NonEphemeralNode = 0L
}

class ZKMasterPathMonitor(connection: CuratorFramework,
                          prefix: String,
                          setMaster: Either[Throwable, Option[SocketHostPortPair]] => Unit)
  extends NodeCacheListener
{

  private val logger = LoggerFactory.getLogger(this.getClass)
  private val nodeToWatch = new NodeCache(
    connection,
    prefix,
    false
  )

  nodeToWatch.getListenable.addListener(this)
  checkOnPathToMasterDoesExist()


  private def validateMaster(node: ChildData) = {
    val hostPort = new String(node.getData)
    val connectionData = connection
      .getZookeeperClient.getCurrentConnectionString

    SocketHostPortPair.fromString(hostPort) match {
      case Some(hostPortPairOpt) =>
        Right(Some(hostPortPairOpt))
      case None =>
        logger.error(s"Master information data ($hostPort) is corrupted for $connectionData$prefix.")
        Left(new MasterDataIsIllegalException(node.getPath, hostPort))
    }
  }

  private def checkOnPathToMasterDoesExist() = {
    val pathToMaster = new File(prefix).getParent
    val isExist =
      Option(connection.checkExists().forPath(pathToMaster)).isDefined
    if (!isExist)
      throw new MasterPathIsAbsent(pathToMaster)
  }

  override def nodeChanged(): Unit = {
    Option(nodeToWatch.getCurrentData) match {
      case Some(node) =>
        if (node.getStat.getEphemeralOwner == ZKMasterPathMonitor.NonEphemeralNode)
          setMaster(Left(new MasterIsPersistentZnodeException(node.getPath)))
        else
          setMaster(validateMaster(node))
      case None =>
        scala.util.Try(checkOnPathToMasterDoesExist()) match {
          case scala.util.Success(_) =>
            setMaster(Right(None))
          case scala.util.Failure(throwable) =>
            setMaster(Left(throwable))
        }
    }
  }


  def startMonitoringMasterServerPath(): Unit = nodeToWatch.start()
  def stopMonitoringMasterServerPath():  Unit = nodeToWatch.close()
}
