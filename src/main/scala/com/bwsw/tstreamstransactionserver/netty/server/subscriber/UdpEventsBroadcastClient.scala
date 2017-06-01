package com.bwsw.tstreamstransactionserver.netty.server.subscriber

import java.net.{DatagramSocket, InetAddress}
import java.util.concurrent.{Executors, TimeUnit}

import com.bwsw.tstreamstransactionserver.protocol.TransactionState
import org.apache.curator.framework.CuratorFramework
import org.slf4j.{Logger, LoggerFactory}
import UdpEventsBroadcastClient._


private object UdpEventsBroadcastClient {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  val UPDATE_PERIOD_MS = 1000
}

/**
  *
  * @param curatorClient
  * @param partitions
  */
final class UdpEventsBroadcastClient(curatorClient: CuratorFramework,
                                     prefix: String,
                                     partitions: Set[Int]
                                    ) {
  private val scheduledExecutor = Executors
    .newSingleThreadScheduledExecutor()

  private val clientSocket =
    new DatagramSocket()

  private val partitionSubscribers =
    new java.util.concurrent.ConcurrentHashMap[Int, java.util.List[String]]()

  @volatile private var isStopped = false

  private val notifySubscribers = new Runnable {
    override def run(): Unit = {
      partitions foreach updateSubscribers
    }
  }

  private def runSubscriberNotificationTask(): Unit = {
    scheduledExecutor.scheduleWithFixedDelay(
      notifySubscribers,
      0L,
      UPDATE_PERIOD_MS,
      TimeUnit.MILLISECONDS
    )
  }

  /**
    * Initialize coordinator
    */
  def init(): Unit = {
    isStopped = true

    partitions.foreach {
      partition => {
        partitionSubscribers.put(
          partition,
          new java.util.LinkedList[String]()
        )
        updateSubscribers(partition)
      }
    }

    //Notify subscribers every 1 sec
    runSubscriberNotificationTask()
  }

  private def broadcast(subscribers: java.util.List[String],
                        message: TransactionState): Unit = {
    if (!subscribers.isEmpty) {
      val binaryMessage = message.toByteArray

      if (UdpEventsBroadcastClient.logger.isDebugEnabled())
        UdpEventsBroadcastClient.logger.debug(
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
              UdpEventsBroadcastClient.logger
                .warn(s"Send $message to $host:$port failed. Exception is: $e")
          }
        }
      })
    }
  }

  /**
    * Publish Message to all accepted subscribers
    *
    * @param message Message
    */
  def publish(message: TransactionState): Unit = {
    if (!isStopped) {
      val subscribers = partitionSubscribers.get(message.partition)
      broadcast(subscribers, message)
    }
  }

  /**
    * Update subscribers on specific partition
    */
  private def updateSubscribers(partition: Int): Unit = {
    scala.util.Try(
      curatorClient.getChildren
        .forPath(s"$prefix/subscribers/$partition")
    ).map(childrenPaths =>
      partitionSubscribers.put(partition, childrenPaths)
    )
  }

  /**
    * Stop this Subscriber client
    */
  def stop(): Unit = {
    if (isStopped) {
      throw new IllegalStateException(
        "Producer->Subscriber notifier was stopped second time."
      )
    } else {
      isStopped = true

      clientSocket.close()
      scheduledExecutor.shutdown()
      scala.util.Try(scheduledExecutor.awaitTermination(
        UPDATE_PERIOD_MS,
        TimeUnit.MILLISECONDS
      ))
      partitionSubscribers.clear()
    }
  }
}

