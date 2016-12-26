package netty.client

import io.netty.channel.ChannelInitializer
import io.netty.channel.socket.SocketChannel
import io.netty.handler.codec.bytes.ByteArrayEncoder
import netty.MessageDecoder

class ClientInitializer extends ChannelInitializer[SocketChannel] {
  override def initChannel(ch: SocketChannel): Unit = {
    ch.pipeline()
      .addLast(new ByteArrayEncoder())
      .addLast(new MessageDecoder)
      .addLast(new ClientHandler)
  }
}
