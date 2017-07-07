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

  @volatile private var isChannelActive = false
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

  private def reconnectOnConnectionLostListener(newChannel: Channel,
                                                bootstrap: Bootstrap) = {
    newChannel.closeFuture.addListener { (_: ChannelFuture) =>
      isChannelActive = false
      newChannel.eventLoop().execute { () =>
        newChannel.close()
        channel = connect(bootstrap)
        isChannelActive = true
      }
    }
  }

  //While loop may be good idea if one pleasures to handle requests without checking if channel is usable
  def getChannel(): Channel = {
    while (!isChannelActive) {}
    channel
  }
}
