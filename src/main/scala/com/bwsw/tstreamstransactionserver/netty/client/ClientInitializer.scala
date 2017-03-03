package com.bwsw.tstreamstransactionserver.netty.client

import com.twitter.scrooge.ThriftStruct
import com.bwsw.tstreamstransactionserver.netty.MessageDecoder
import com.google.common.cache.Cache
import io.netty.channel.ChannelInitializer
import io.netty.channel.socket.SocketChannel
import io.netty.handler.codec.bytes.ByteArrayEncoder

import scala.concurrent.{ExecutionContext, Promise => ScalaPromise}

class ClientInitializer(reqIdToRep: Cache[Integer, ScalaPromise[ThriftStruct]], client: Client, context: ExecutionContext) extends ChannelInitializer[SocketChannel] {
  override def initChannel(ch: SocketChannel): Unit = {
    ch.pipeline()
      .addLast(new ByteArrayEncoder())
      .addLast(new MessageDecoder)
      .addLast(new ClientHandler(reqIdToRep, client, context))
  }
}



