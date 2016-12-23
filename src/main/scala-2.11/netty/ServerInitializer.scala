package netty

import io.netty.channel.ChannelInitializer
import io.netty.channel.socket.SocketChannel


class ServerInitializer extends ChannelInitializer[SocketChannel] {
  override def initChannel(ch: SocketChannel): Unit = {
    val pipeline = ch.pipeline()
    pipeline.addLast(new MessageDecoder)
    pipeline.addLast(new ServerHandler)
  }
}
