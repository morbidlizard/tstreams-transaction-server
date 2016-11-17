import org.scalatest.{FlatSpec, Matchers}
import zooKeeper.{Agent, StreamLeaderSelectorPriority, Tree, ZooTree}

class TreeTest extends FlatSpec with Matchers {

  "An agent" should "be added to a partition of a stream and take the leadership when the partition doesn't contain agents at all" in {
    val zookeeperConnectionString = "172.17.0.2:2181"

    val zooTree = new ZooTree(zookeeperConnectionString)

    val stream = zooTree.addStream("test_stream", 12)

    val agent1 = Agent("123.123.3.1","1444","5", Agent.Priority.Normal)
    zooTree.addAgent(agent1,"test_stream", Seq(1,2,3,10,11))

    val agent2 = Agent("123.123.3.11","1444","5", Agent.Priority.Low)
    zooTree.addAgent(agent2,"test_stream", Seq(1,2,3,10,11))

    println(zooTree.getStreamPartitionLeader("test_stream",10))

    zooTree.close()
  }

}
