package netty.client

import scala.concurrent.{Promise => ScalaPromise}

//class ClientInitializer(reqIdToRep: ConcurrentHashMap[Int, ScalaPromise[ThriftStruct]], client: Client, context: ExecutionContext) extends ChannelInitializer[SocketChannel] {
//  override def initChannel(ch: SocketChannel): Unit = {
//    ch.pipeline()
//      //.addLast(new ClientTestHandler(client))
//      .addLast(new ByteArrayEncoder())
//      .addLast(new MessageDecoder)
//      .addLast(new ClientHandler(reqIdToRep, client, context))
//  }
//}



