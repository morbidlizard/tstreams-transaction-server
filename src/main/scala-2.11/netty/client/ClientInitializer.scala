package netty.client

import java.util.concurrent.ConcurrentHashMap

import io.netty.channel.ChannelInitializer
import io.netty.channel.socket.SocketChannel
import io.netty.handler.codec.bytes.ByteArrayEncoder
import netty.MessageDecoder

import scala.concurrent.{Promise => ScalaPromise}

class ClientInitializer(reqIdToRep: ConcurrentHashMap[Int, ScalaPromise[FunctionResult.Result]]) extends ChannelInitializer[SocketChannel] {
  override def initChannel(ch: SocketChannel): Unit = {
    ch.pipeline()
      .addLast(new ByteArrayEncoder())
      .addLast(new MessageDecoder)
      .addLast(new ClientHandler(reqIdToRep))
  }
}
