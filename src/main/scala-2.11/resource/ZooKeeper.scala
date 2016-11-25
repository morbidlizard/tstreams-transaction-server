package resource

import zooKeeper.LeaderSelectorPriority.Priority
import zooKeeper.{LeaderSelectorPriorityClient, LeaderSelectorPriorityServer}

object ZooKeeper {
  val Address = "172.17.0.2:2181"
  val SessionTimeout: Int = java.util.concurrent.TimeUnit.SECONDS.toMillis(5).toInt
  val ConnectionTimeout: Int = java.util.concurrent.TimeUnit.SECONDS.toMillis(5).toInt
  val TimeoutBetweenRetries: Int = java.util.concurrent.TimeUnit.SECONDS.toMillis(1).toInt
  val MaxRetriesNumber: Int = 20
  val Prefix = "/stream"
}
