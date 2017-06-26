package ut

import com.bwsw.tstreamstransactionserver.netty.server.bookkeeperService.hierarchy.ZookeeperTreeListLong
import org.scalatest.{FlatSpec, Matchers}
import util.Utils

class ZookeeperTreeListTest
  extends FlatSpec
    with Matchers
{

  "ZookeeperTreeListLong" should "return first entry id and last entry id as Nones" in {
    val (zkServer, zkClient) = Utils.startZkServerAndGetIt

    val treeListLong = new ZookeeperTreeListLong(zkClient, "/test")

    treeListLong.lastEntityID shouldBe None
    treeListLong.firstEntityID shouldBe None

    zkClient.close()
    zkServer.close()
  }

  it should "return first entry id and last entry id the same as only one entity id was persisted" in {
    val (zkServer, zkClient) = Utils.startZkServerAndGetIt

    val treeListLong = new ZookeeperTreeListLong(zkClient, "/test")

    val value = 1L
    treeListLong.createNode(value)

    treeListLong.firstEntityID shouldBe defined
    treeListLong.firstEntityID.get == value

    treeListLong.firstEntityID shouldBe treeListLong.lastEntityID

    zkClient.close()
    zkServer.close()
  }

  it should "return first entry id and last entry id properly" in {
    val (zkServer, zkClient) = Utils.startZkServerAndGetIt

    val treeListLong = new ZookeeperTreeListLong(zkClient, "/test")

    val startNumber = 0
    val maxNumbers  = 10

    (startNumber to maxNumbers)
      .foreach(number => treeListLong.createNode(number))

    treeListLong.firstEntityID shouldBe Some(startNumber)
    treeListLong.lastEntityID  shouldBe Some(maxNumbers)

    zkClient.close()
    zkServer.close()
  }

  it should "return a next node of some node properly" in {
    val (zkServer, zkClient) = Utils.startZkServerAndGetIt

    val treeListLong = new ZookeeperTreeListLong(zkClient, "/test")

    val startNumber = 0
    val maxNumbers  = 10

    (startNumber to maxNumbers)
      .foreach(number => treeListLong.createNode(number))

    val id = scala.util.Random.nextInt(maxNumbers)
    treeListLong.getNextNode(id) shouldBe Some(id + 1)


    zkClient.close()
    zkServer.close()
  }

  it should "not return a next node of last node" in {
    val (zkServer, zkClient) = Utils.startZkServerAndGetIt

    val treeListLong = new ZookeeperTreeListLong(zkClient, "/test")

    val startNumber = 0
    val maxNumbers  = 10

    (startNumber to maxNumbers)
      .foreach(number => treeListLong.createNode(number))

    val id = maxNumbers
    treeListLong.getNextNode(id) shouldBe None


    zkClient.close()
    zkServer.close()
  }

  it should "return a previous node of some node correctly" in {
    val (zkServer, zkClient) = Utils.startZkServerAndGetIt

    val treeListLong = new ZookeeperTreeListLong(zkClient, "/test")

    val startNumber = 0
    val maxNumbers  = 10

    (startNumber to maxNumbers)
      .foreach(number => treeListLong.createNode(number))

    val id = scala.util.Random.nextInt(maxNumbers + 1)
    treeListLong.getPreviousNode(id) shouldBe Some(id - 1)

    zkClient.close()
    zkServer.close()
  }

  it should "not return a previous node that doesn't exit" in {
    val (zkServer, zkClient) = Utils.startZkServerAndGetIt

    val treeListLong = new ZookeeperTreeListLong(zkClient, "/test")

    val startNumber = 0
    val maxNumbers  = 10

    (startNumber to maxNumbers)
      .foreach(number => treeListLong.createNode(number))

    val id = -1
    treeListLong.getPreviousNode(id) shouldBe None

    zkClient.close()
    zkServer.close()
  }

  it should "delete the one node tree list correctly" in {
    val (zkServer, zkClient) = Utils.startZkServerAndGetIt

    val treeListLong = new ZookeeperTreeListLong(zkClient, "/test")

    val startNumber = 0
    val maxNumbers  = 0

    val ids = (startNumber to maxNumbers).toArray

    ids.foreach(id =>
      treeListLong.createNode(id)
    )

    val id = 0
    treeListLong.deleteNode(id) shouldBe true

    treeListLong.firstEntityID shouldBe None
    treeListLong.lastEntityID  shouldBe None

    zkClient.close()
    zkServer.close()
  }

  it should "delete the first node correctly" in {
    val (zkServer, zkClient) = Utils.startZkServerAndGetIt

    val treeListLong = new ZookeeperTreeListLong(zkClient, "/test")

    val startNumber = 0
    val maxNumbers  = 7

    val ids = (startNumber to maxNumbers).toArray

    ids.foreach(id =>
      treeListLong.createNode(id)
    )

    val firstID = startNumber
    val nextID  = startNumber + 1
    treeListLong.deleteNode(firstID) shouldBe true

    treeListLong.firstEntityID shouldBe Some(nextID)
    treeListLong.lastEntityID  shouldBe Some(maxNumbers)


    zkClient.close()
    zkServer.close()
  }

  it should "delete the last node correctly" in {
    val (zkServer, zkClient) = Utils.startZkServerAndGetIt

    val treeListLong = new ZookeeperTreeListLong(zkClient, "/test")

    val startNumber = 0
    val maxNumbers  = 7

    val ids = (startNumber to maxNumbers).toArray

    ids.foreach(id =>
      treeListLong.createNode(id)
    )

    val lastID = maxNumbers
    val previousID = lastID - 1
    treeListLong.deleteNode(lastID) shouldBe true
    treeListLong.getNextNode(previousID) shouldBe None

    treeListLong.firstEntityID shouldBe Some(startNumber)
    treeListLong.lastEntityID  shouldBe Some(previousID)

    zkClient.close()
    zkServer.close()
  }


  it should "delete a node between first entity id and last entity id" in {
    val (zkServer, zkClient) = Utils.startZkServerAndGetIt

    val treeListLong = new ZookeeperTreeListLong(zkClient, "/test")

    val startNumber = 0
    val maxNumbers  = 10
    val moveRangeToLeftBoundNumber = 2

    val ids = (startNumber to maxNumbers).toArray

    ids.foreach(id =>
      treeListLong.createNode(id)
    )

    val id = scala.util.Random.nextInt(
      maxNumbers - moveRangeToLeftBoundNumber
    ) + moveRangeToLeftBoundNumber

    val previousID = id - 1
    val nextID = id + 1
    treeListLong.deleteNode(id) shouldBe true
    treeListLong.getNextNode(previousID - 1) shouldBe Some(previousID)
    treeListLong.getNextNode(previousID) shouldBe Some(nextID)

    treeListLong.firstEntityID shouldBe Some(startNumber)
    treeListLong.lastEntityID  shouldBe Some(maxNumbers)

    zkClient.close()
    zkServer.close()
  }

  it should "delete nodes from [head, n], n - some positive number" in {
    val (zkServer, zkClient) = Utils.startZkServerAndGetIt

    val treeListLong = new ZookeeperTreeListLong(zkClient, "/test")

    val startNumber = 0
    val maxNumbers  = 30

    val ids = (startNumber to maxNumbers).toArray

    ids.foreach(id =>
      treeListLong.createNode(id)
    )

    val number = scala.util.Random.nextInt(maxNumbers)
    treeListLong.deleteLeftNodes(number)

    treeListLong.firstEntityID shouldBe Some(number)
    treeListLong.lastEntityID  shouldBe Some(maxNumbers)

    zkClient.close()
    zkServer.close()
  }

  it should "delete nodes from [head, n], n - some positive number, which is greater than number of nodes list contains" in {
    val (zkServer, zkClient) = Utils.startZkServerAndGetIt

    val treeListLong = new ZookeeperTreeListLong(zkClient, "/test")

    val startNumber = 0
    val maxNumbers  = 15

    val ids = (startNumber to maxNumbers).toArray

    ids.foreach(id =>
      treeListLong.createNode(id)
    )

    val number = maxNumbers + 1
    treeListLong.deleteLeftNodes(number)

    treeListLong.firstEntityID shouldBe None
    treeListLong.lastEntityID  shouldBe None

    zkClient.close()
    zkServer.close()
  }
}
