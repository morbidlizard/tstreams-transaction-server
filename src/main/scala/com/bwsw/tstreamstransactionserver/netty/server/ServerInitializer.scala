package com.bwsw.tstreamstransactionserver.netty.server

import com.bwsw.tstreamstransactionserver.netty.MessageDecoder
import com.bwsw.tstreamstransactionserver.netty.server.commitLogService.JournaledCommitLogImpl
import io.netty.channel.ChannelInitializer
import io.netty.channel.socket.SocketChannel
import io.netty.handler.codec.bytes.ByteArrayEncoder
import org.slf4j.Logger

import scala.concurrent.ExecutionContext


class ServerInitializer(server: TransactionServer, journaledCommitLog: JournaledCommitLogImpl, context: ExecutionContext, logger: Logger) extends ChannelInitializer[SocketChannel] {
  override def initChannel(ch: SocketChannel): Unit = {
    ch.pipeline()
      .addLast(new ByteArrayEncoder())
      .addLast(new MessageDecoder)
      .addLast(new ServerHandler(server, journaledCommitLog, context, logger))
  }
}
