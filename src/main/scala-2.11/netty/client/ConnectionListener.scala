package netty.client

import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

import configProperties.ClientConfig.{serverTimeoutBetweenRetries, serverTimeoutConnection}
import io.netty.bootstrap.Bootstrap
import io.netty.channel.{ChannelFuture, ChannelFutureListener}

class ConnectionListener(client: Client) extends ChannelFutureListener() {
  val atomicInteger = new AtomicInteger(serverTimeoutConnection / serverTimeoutBetweenRetries)

  @throws[Exception]
  override def operationComplete(channelFuture: ChannelFuture): Unit = {
    if (!channelFuture.isSuccess) {
      System.out.println("Reconnect")
      val loop = channelFuture.channel().eventLoop()
      loop.schedule(new Runnable() {
        override def run() {
          client.createBootstrap(new Bootstrap(), loop)
        }
      }, 1L, TimeUnit.SECONDS)
    } else {
      client.channel = channelFuture.sync().channel()
      println(client.channel)
    }
    // } else if (atomicInteger.get() <= 0) throw new ServerConnectionException
    //    if (!channelFuture.isSuccess && atomicInteger.getAndDecrement() > 0) {
    //      channelFuture.channel().close()
    //      TimeUnit.MILLISECONDS.sleep(serverTimeoutBetweenRetries)
    //      channelFuture.addListener(this)
    //    } else if (atomicInteger.get() <= 0) throw new ServerConnectionException
  }
}
