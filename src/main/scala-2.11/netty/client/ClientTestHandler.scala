package netty.client

//class ClientTestHandler(client: Client) extends ChannelInboundHandlerAdapter {
//  override def channelInactive(ctx: ChannelHandlerContext): Unit = {
//    val ctxChannel = ctx.channel()
//    if (!ctxChannel.eventLoop().isShuttingDown) {
//      val reconnect = new ReconnectionTask(client, ctxChannel)
//      reconnect.run()
//    }
//  }
//}
//
//class ReconnectionTask(client: Client, ctxChannel: Channel) extends Runnable with ChannelFutureListener {
//  override def run(): Unit = {
//    val (listen, port) = client.getInetAddressFromZookeeper(5)
//    client.channelGroup.add(client.bootstrap.remoteAddress(listen, port)
//      .connect()
//      .addListener(this)
//      .sync()
//      .channel()
//    )
//  }
//
//  override def operationComplete(future: ChannelFuture): Unit = {
//    if (!future.isSuccess) {
//      ctxChannel.eventLoop().schedule(this, 30, java.util.concurrent.TimeUnit.MILLISECONDS)
//    }
//  }
//}
