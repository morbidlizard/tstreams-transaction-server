package netty.client

import java.util.concurrent.ConcurrentHashMap

import com.twitter.scrooge.ThriftStruct
import io.netty.bootstrap.Bootstrap
import io.netty.channel.{Channel, ChannelInitializer}
import io.netty.channel.socket.SocketChannel
import io.netty.handler.codec.bytes.ByteArrayEncoder
import netty.MessageDecoder
import zooKeeper.ZKLeaderClient

import scala.concurrent.{ExecutionContext, Promise => ScalaPromise}

class ClientInitializer(reqIdToRep: ConcurrentHashMap[Int, ScalaPromise[ThriftStruct]], client: Client, context: ExecutionContext) extends ChannelInitializer[SocketChannel] {
  override def initChannel(ch: SocketChannel): Unit = {
    ch.pipeline()
      .addLast(new ClientTestHandler(client))
      .addLast(new ByteArrayEncoder())
      .addLast(new MessageDecoder)
      .addLast(new ClientHandler(reqIdToRep, context))
  }
}



