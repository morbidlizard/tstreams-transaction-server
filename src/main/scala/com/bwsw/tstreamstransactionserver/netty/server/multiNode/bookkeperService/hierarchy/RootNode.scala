package com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService.hierarchy

import org.apache.curator.framework.CuratorFramework
import org.apache.zookeeper.KeeperException

private object RootNode {
  val delimiterIndexFieldSize = java.lang.Integer.BYTES
}

class RootNode(client: CuratorFramework,
               rootPath: String) {

  private var nodeData = init()

  final def getData: RootNodeData = nodeData

  final def setFirstAndLastIDInRootNode(first: Array[Byte],
                                        second: Array[Byte]): Unit = {
    val buf = java.nio.ByteBuffer
      .allocate(RootNode.delimiterIndexFieldSize)
      .putInt(first.length)
    buf.flip()


    val binaryIndex = new Array[Byte](RootNode.delimiterIndexFieldSize)
    buf.get(binaryIndex)

    val data = first ++ binaryIndex ++ second

    client.setData()
      .forPath(rootPath, data)

    nodeData = RootNodeData(
      first,
      second
    )
  }

  private def init(): RootNodeData = {
    scala.util.Try {
      client.getData
        .forPath(rootPath)
    }.map { data =>
      if (data.isEmpty)
        RootNodeData(
          Array.emptyByteArray,
          Array.emptyByteArray
        )
      else {
        val (delimiter, ids) =
          data.splitAt(RootNode.delimiterIndexFieldSize)

        val indexToSplitIDs =
          java.nio.ByteBuffer.wrap(delimiter).getInt

        val (firstID, secondID) =
          ids.splitAt(indexToSplitIDs)

        RootNodeData(
          firstID,
          secondID
        )
      }
    } match {
      case scala.util.Success(nodeData) =>
        nodeData
      case scala.util.Failure(throwable) =>
        throwable match {
          case _: KeeperException.NoNodeException =>
            client.create()
              .creatingParentsIfNeeded()
              .forPath(rootPath, Array.emptyByteArray)

            RootNodeData(
              Array.emptyByteArray,
              Array.emptyByteArray
            )
        }
    }
  }
}
