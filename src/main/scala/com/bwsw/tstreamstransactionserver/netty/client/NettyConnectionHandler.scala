package com.bwsw.tstreamstransactionserver.netty.client

import java.util.concurrent.atomic.AtomicBoolean

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
  private val isReconnecting = new AtomicBoolean(false)

  @volatile private var isStopped = false
  @volatile private var channel: Channel = connect()

  private def determineChannelType(): Class[_ <: SocketChannel] =
    workerGroup match {
      case _: EpollEventLoopGroup => classOf[EpollSocketChannel]
      case _: NioEventLoopGroup => classOf[NioSocketChannel]
      case group => throw new IllegalArgumentException(
        s"Can't determine channel type for group '$group'."
      )
    }

  @tailrec
  final private def connect(): Channel = {
    val socket = getConnectionAddress
    val newConnection = bootstrap.connect(socket.address, socket.port)
    scala.util.Try {
      newConnection.syncUninterruptibly()
      newConnection.channel()
    } match {
      case scala.util.Success(channelToUse) =>
        if (channelToUse.isActive) {
          connectionLostListener(channelToUse)
          channelToUse
        } else {
          channelToUse.close().awaitUninterruptibly()
          onConnectionLostDo
          connect()
        }
      case scala.util.Failure(throwable) =>
        if (throwable.isInstanceOf[java.util.concurrent.RejectedExecutionException])
          throw ClientIllegalOperationAfterShutdown
        else {
          onConnectionLostDo
          connect()
        }
    }
  }

  final def reconnect(): Unit = {
    def go() {
      val socket = getConnectionAddress
      val newConnection = bootstrap.connect(socket.address, socket.port)
      newConnection.addListener { (futureChannel: ChannelFuture) =>
        val channel = futureChannel.channel()
        if (!isStopped && futureChannel.isSuccess) {
          if (channel.isActive && channel.isOpen) {
            this.channel = channel
            isReconnecting.set(false)
            connectionLostListener(channel)
          } else {
            channel.close()
            onConnectionLostDo
            go()
          }
        }
        else {
          onConnectionLostDo
          val eventLoop = channel.eventLoop()
          if (!isStopped && !eventLoop.isShuttingDown) {
            go()
          }
        }
      }
    }

    val needToReconnect =
      isReconnecting.compareAndSet(false, true)

    if (needToReconnect)
      go()
    else while (isReconnecting.get()) {}
  }


  private def connectionLostListener(oldChannel: Channel) = {
    oldChannel.closeFuture.addListener { (f: ChannelFuture) =>
      val eventLoop = f.channel().eventLoop()
      if (!isStopped && !eventLoop.isShuttingDown) {
        eventLoop.execute { () =>
          onConnectionLostDo
          reconnect()
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
      channel.close().awaitUninterruptibly()
    }
}
