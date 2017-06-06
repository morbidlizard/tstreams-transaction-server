package com.bwsw.tstreamstransactionserver.netty.server.subscriber

import java.net.{DatagramSocket, InetAddress}

import com.bwsw.tstreamstransactionserver.protocol.TransactionState
import org.slf4j.{Logger, LoggerFactory}

private object SubscriberNotifier {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)
}


private[server] class SubscriberNotifier {
  @volatile private var isStopped = false

  private val clientSocket =
    new DatagramSocket()

  def broadcast(subscribers: java.util.Collection[String],
                message: TransactionState): Unit = {
    if (subscribers.isEmpty || isStopped) {
      //do nothing
    } else {
      val binaryMessage = message.toByteArray

      if (SubscriberNotifier.logger.isDebugEnabled())
        SubscriberNotifier.logger.debug(
          s"Subscribers to broadcast: $subscribers"
        )

      subscribers.forEach(address => {
        val addressPort = address.split(":")
        val host = InetAddress.getByName(addressPort(0))
        val port = addressPort(1).toInt

        val sendPacket =
          new java.net.DatagramPacket(
            binaryMessage,
            binaryMessage.length,
            host,
            port
          )

        var isSent = false
        while (!isSent) {
          try {
            clientSocket.send(sendPacket)
            isSent = true
          } catch {
            case e: Exception =>
              SubscriberNotifier.logger
                .warn(s"Send $message to $host:$port failed. Exception is: $e")
          }
        }
      })
    }
  }

  def close(): Unit = {
    if (isStopped) {
      throw new IllegalStateException(
        "Socket is closed"
      )
    } else {
      isStopped = true
      clientSocket.close()
    }
  }

}
