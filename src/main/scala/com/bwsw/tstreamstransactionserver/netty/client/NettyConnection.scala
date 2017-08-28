package com.bwsw.tstreamstransactionserver.netty.client

import java.util.concurrent.atomic.AtomicBoolean

import com.bwsw.tstreamstransactionserver.netty.SocketHostPortPair
import io.netty.bootstrap.Bootstrap
import io.netty.channel.epoll.{EpollEventLoopGroup, EpollSocketChannel}
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.SocketChannel
import io.netty.channel.socket.nio.NioSocketChannel
import io.netty.channel.{ChannelInitializer, _}
import io.netty.handler.logging.{LogLevel, LoggingHandler}
import org.slf4j.LoggerFactory


class NettyConnection(workerGroup: EventLoopGroup,
                      handlers: => Seq[ChannelHandler],
                      connectionTimeoutMs: Int,
                      reconnectDelayMs: Int,
                      getConnectionAddress: => SocketHostPortPair,
                      onConnectionLostDo: => Unit) {
  private val logger =
    LoggerFactory.getLogger(this.getClass)

  private val isStopped =
    new AtomicBoolean(false)
  @volatile private var channel: ChannelFuture = {
    val socket = getConnectionAddress
    bootstrap.connect(socket.address, socket.port)
  }

  final def reconnect(): Unit = {
    channel.channel().deregister()
  }

  def getChannel(): Channel = {
    channel.channel()
  }

  def stop(): Unit = {
    val isNotStopped =
      isStopped.compareAndSet(false, true)
    if (isNotStopped) {
      scala.util.Try(
        channel.channel()
          .close()
          .cancel(true)
      )
    }
  }

  private def bootstrap: Bootstrap = {
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
      .handler(new ChannelInitializer[SocketChannel] {
        override def initChannel(ch: SocketChannel): Unit = {
          val pipeline = ch.pipeline()

          handlers.foreach(handler =>
            pipeline.addLast(handler)
          )

          pipeline.addFirst(
            new LoggingHandler(LogLevel.DEBUG)
          )

          pipeline.addLast(
            new NettyConnectionHandler(
              reconnectDelayMs,
              onConnectionLostDo,
              connect()
            ))
        }
      })
  }

  private def determineChannelType(): Class[_ <: SocketChannel] =
    workerGroup match {
      case _: EpollEventLoopGroup => classOf[EpollSocketChannel]
      case _: NioEventLoopGroup => classOf[NioSocketChannel]
      case group => throw new IllegalArgumentException(
        s"Can't determine channel type for group '$group'."
      )
    }

  private final def connect() = {
    val socket = getConnectionAddress
    bootstrap.connect(
      socket.address,
      socket.port
    ).addListener { (futureChannel: ChannelFuture) =>
      if (futureChannel.cause() != null) {
        if (logger.isInfoEnabled)
          logger.debug(s"Failed to connect: ${socket.address}:${socket.port}, cause: ${futureChannel.cause}")
      }
      else {
        if (logger.isInfoEnabled)
          logger.debug(s"Connected to: ${futureChannel.channel().remoteAddress()}")
        channel = futureChannel
      }
    }
  }
}
