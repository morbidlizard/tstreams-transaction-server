package com.bwsw.tstreamstransactionserver.netty.client

import com.bwsw.tstreamstransactionserver.exception.Throwable.ClientIllegalOperationAfterShutdown
import com.bwsw.tstreamstransactionserver.netty.SocketHostPortPair
import io.netty.bootstrap.Bootstrap
import io.netty.channel.{ChannelInitializer, _}
import io.netty.channel.epoll.{EpollEventLoopGroup, EpollSocketChannel}
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.SocketChannel
import io.netty.channel.socket.nio.NioSocketChannel

import scala.annotation.tailrec

class NettyConnectionHandler(workerGroup: EventLoopGroup,
                             handlersChain: ChannelInitializer[SocketChannel],
                             connectionTimeoutMs: Int,
                             getConnectionAddress: => SocketHostPortPair,
                             onConnectionLostDo:   => Unit) {

  private val bootstrap: Bootstrap = {
    new Bootstrap()
      .group(workerGroup)
      .channel(determineChannelType())
      .option(
        ChannelOption.SO_KEEPALIVE,
        java.lang.Boolean.FALSE
      )
      .option(
        ChannelOption.CONNECT_TIMEOUT_MILLIS,
        int2Integer(connectionTimeoutMs)
      )
      .handler(handlersChain)
  }

  @volatile private var isStopped = false
  @volatile private var channel: Channel = connect(bootstrap)


  private def determineChannelType(): Class[_ <: SocketChannel] =
    workerGroup match {
      case _: EpollEventLoopGroup => classOf[EpollSocketChannel]
      case _: NioEventLoopGroup => classOf[NioSocketChannel]
      case group => throw new IllegalArgumentException(
        s"Can't determine channel type for group '$group'."
      )
    }


  @tailrec
  final private def connect(bootstrap: Bootstrap): Channel = {
    val socket = getConnectionAddress
    val newConnection = bootstrap.connect(socket.address, socket.port)
    scala.util.Try {
      newConnection.sync().channel()
    } match {
      case scala.util.Success(channelToUse) =>
        reconnectOnConnectionLostListener(
          channelToUse,
          bootstrap
        )
        channelToUse
      case scala.util.Failure(throwable) =>
        if (throwable.isInstanceOf[java.util.concurrent.RejectedExecutionException])
          throw ClientIllegalOperationAfterShutdown
        else {
          onConnectionLostDo
          connect(bootstrap)
        }
    }
  }

  final def reconnect(): Unit = {
    channel.close().awaitUninterruptibly()
  }

  private def reconnectOnConnectionLostListener(oldChannel: Channel,
                                                bootstrap: Bootstrap) = {
    oldChannel.closeFuture.addListener { (_: ChannelFuture) =>
      if (!isStopped) {
        oldChannel.eventLoop().execute { () =>
          oldChannel.close()
          onConnectionLostDo
          channel = connect(bootstrap)
        }
      }
    }
  }

  def getChannel(): Channel = {
    channel
  }

  def stop(): Unit =
    this.synchronized {
      isStopped = true
      reconnect()
    }
}
