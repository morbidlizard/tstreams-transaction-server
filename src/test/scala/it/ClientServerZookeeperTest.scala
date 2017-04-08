package it

import java.io.File
import java.util
import java.util.concurrent.TimeUnit

import com.bwsw.tstreamstransactionserver.exception.Throwable.{InvalidSocketAddress, ZkGetMasterException, ZkNoConnectionException}
import com.bwsw.tstreamstransactionserver.netty.InetSocketAddressClass
import com.bwsw.tstreamstransactionserver.netty.server.Server
import com.bwsw.tstreamstransactionserver.options.CommonOptions.ZookeeperOptions
import com.bwsw.tstreamstransactionserver.options.ServerOptions.{BootstrapOptions, StorageOptions}
import com.bwsw.tstreamstransactionserver.options.{ClientBuilder, ServerBuilder}
import org.apache.commons.io.FileUtils
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.retry.RetryForever
import org.apache.curator.test.TestingServer
import org.apache.zookeeper.CreateMode
import org.apache.zookeeper.ZooDefs.{Ids, Perms}
import org.apache.zookeeper.data.ACL
import org.scalatest.{FlatSpec, Matchers}

class ClientServerZookeeperTest extends FlatSpec with Matchers {

  "Client" should "not connect to zookeeper server that isn't running" in {
    val clientBuilder = new ClientBuilder().withZookeeperOptions(ZookeeperOptions(endpoints = "127.0.0.1:8888", connectionTimeoutMs = 2000))
    assertThrows[ZkNoConnectionException] {
      clientBuilder.build()
    }
  }

  it should "not connect to server which socket address(retrieved from zooKeeper server) is wrong" in {
    val zkPrefix = "/tts"
    val zkTestServer = new TestingServer(true)

    val zkClient = CuratorFrameworkFactory.builder()
      .sessionTimeoutMs(1000)
      .connectionTimeoutMs(1000)
      .retryPolicy(new RetryForever(100))
      .connectString(zkTestServer.getConnectString)
      .build()
    zkClient.start()
    zkClient.blockUntilConnected(1, TimeUnit.SECONDS)

    val permissions = new util.ArrayList[ACL]()
    permissions.add(new ACL(Perms.READ, Ids.ANYONE_ID_UNSAFE))
    zkClient.create().creatingParentsIfNeeded()
      .withMode(CreateMode.EPHEMERAL)
      .withACL(permissions)
      .forPath(zkPrefix, "Test".getBytes())

    val clientBuilder = new ClientBuilder()
      .withZookeeperOptions(ZookeeperOptions(endpoints = zkTestServer.getConnectString, prefix = zkPrefix))

    assertThrows[ZkGetMasterException ] {
      clientBuilder.build()
    }

    zkTestServer.close()
  }

  it should "not connect to server which inet address(retrieved from zooKeeper server) is wrong" in {
    val zkPrefix = "/tts"
    val zkTestServer = new TestingServer(true)

    val zkClient = CuratorFrameworkFactory.builder()
      .sessionTimeoutMs(1000)
      .connectionTimeoutMs(1000)
      .retryPolicy(new RetryForever(100))
      .connectString(zkTestServer.getConnectString)
      .build()
    zkClient.start()
    zkClient.blockUntilConnected(1, TimeUnit.SECONDS)

    val permissions = new util.ArrayList[ACL]()
    permissions.add(new ACL(Perms.READ, Ids.ANYONE_ID_UNSAFE))
    zkClient.create().creatingParentsIfNeeded()
      .withMode(CreateMode.EPHEMERAL)
      .withACL(permissions)
      .forPath(zkPrefix, "1270.0.0.1:8080".getBytes())

    val clientBuilder = new ClientBuilder()
      .withZookeeperOptions(ZookeeperOptions(endpoints = zkTestServer.getConnectString, prefix = zkPrefix))

    assertThrows[ZkGetMasterException ] {
      clientBuilder.build()
    }

    zkTestServer.close()
  }

  it should "not connect to server which port value(retrieved from zooKeeper server) is negative" in {
    val zkPrefix = "/tts"
    val zkTestServer = new TestingServer(true)

    val zkClient = CuratorFrameworkFactory.builder()
      .sessionTimeoutMs(1000)
      .connectionTimeoutMs(1000)
      .retryPolicy(new RetryForever(100))
      .connectString(zkTestServer.getConnectString)
      .build()
    zkClient.start()
    zkClient.blockUntilConnected(1, TimeUnit.SECONDS)

    val permissions = new util.ArrayList[ACL]()
    permissions.add(new ACL(Perms.READ, Ids.ANYONE_ID_UNSAFE))
    zkClient.create().creatingParentsIfNeeded()
      .withMode(CreateMode.EPHEMERAL)
      .withACL(permissions)
      .forPath(zkPrefix, "1270.0.0.1:-8080".getBytes())

    val clientBuilder = new ClientBuilder()
      .withZookeeperOptions(ZookeeperOptions(endpoints = zkTestServer.getConnectString, prefix = zkPrefix))

    assertThrows[ZkGetMasterException ] {
      clientBuilder.build()
    }

    zkTestServer.close()
  }

  it should "not connect to server which port value(retrieved from zooKeeper server) exceeds 65535" in {
    val zkPrefix = "/tts"
    val zkTestServer = new TestingServer(true)

    val zkClient = CuratorFrameworkFactory.builder()
      .sessionTimeoutMs(1000)
      .connectionTimeoutMs(1000)
      .retryPolicy(new RetryForever(100))
      .connectString(zkTestServer.getConnectString)
      .build()
    zkClient.start()
    zkClient.blockUntilConnected(1, TimeUnit.SECONDS)

    val permissions = new util.ArrayList[ACL]()
    permissions.add(new ACL(Perms.READ, Ids.ANYONE_ID_UNSAFE))
    zkClient.create().creatingParentsIfNeeded()
      .withMode(CreateMode.EPHEMERAL)
      .withACL(permissions)
      .forPath(zkPrefix, "1270.0.0.1:65536".getBytes())

    val clientBuilder = new ClientBuilder()
      .withZookeeperOptions(ZookeeperOptions(endpoints = zkTestServer.getConnectString, prefix = zkPrefix))

    assertThrows[ZkGetMasterException ] {
      clientBuilder.build()
    }

    zkTestServer.close()
  }

  //error prone test as uses thread sleeps to change state of a client connection.
  it should "connect to server, and when the server shutdown, starts on another port — client should reconnect properly" in {
    val zkTestServer = new TestingServer(true)

    val serverBuilder = new ServerBuilder()
    val clientBuilder = new ClientBuilder()
      .withZookeeperOptions(ZookeeperOptions(endpoints = zkTestServer.getConnectString))


    var server: Server = null
    val storageOptions = StorageOptions()
    def startTransactionServer(newHost: String, newPort: Int) = new Thread(() => {
      server = serverBuilder
        .withServerStorageOptions(storageOptions)
        .withZookeeperOptions(ZookeeperOptions(endpoints = zkTestServer.getConnectString))
        .withBootstrapOptions(BootstrapOptions(host = newHost, port = newPort))
        .build()
      server.start()
    }).start()


    val host = "127.0.0.1"
    val initialPort = 8071
    val newPort = 8073

    startTransactionServer(host, initialPort)

    Thread.sleep(500)
    val client = clientBuilder.build()

    val initialSocketAddress = client.currentConnectionSocketAddress.get
    server.shutdown()
    startTransactionServer(host, newPort)


    Thread.sleep(500)
    val newSocketAddress = client.currentConnectionSocketAddress.get

    initialSocketAddress shouldBe InetSocketAddressClass(host, initialPort)
    newSocketAddress     shouldBe InetSocketAddressClass(host, newPort)

    zkTestServer.close()
    server.shutdown()

    FileUtils.deleteDirectory(new File(storageOptions.path + java.io.File.separatorChar + storageOptions.metadataDirectory))
    FileUtils.deleteDirectory(new File(storageOptions.path + java.io.File.separatorChar + storageOptions.dataDirectory))
    FileUtils.deleteDirectory(new File(storageOptions.path + java.io.File.separatorChar + storageOptions.commitLogRocksDirectory))
    FileUtils.deleteDirectory(new File(storageOptions.path + java.io.File.separatorChar + storageOptions.commitLogDirectory))
  }


  "Server" should "not connect to zookeeper server that isn't running" in {
    val storageOptions = StorageOptions()
    val serverBuilder = new ServerBuilder()
      .withServerStorageOptions(storageOptions)
      .withZookeeperOptions(ZookeeperOptions(endpoints = "127.0.0.1:8888", connectionTimeoutMs = 2000))

    assertThrows[ZkNoConnectionException] {
      serverBuilder.build()
    }

    FileUtils.deleteDirectory(new File(storageOptions.path + java.io.File.separatorChar + storageOptions.metadataDirectory))
    FileUtils.deleteDirectory(new File(storageOptions.path + java.io.File.separatorChar + storageOptions.dataDirectory))
    FileUtils.deleteDirectory(new File(storageOptions.path + java.io.File.separatorChar + storageOptions.commitLogRocksDirectory))
    FileUtils.deleteDirectory(new File(storageOptions.path + java.io.File.separatorChar + storageOptions.commitLogDirectory))
  }

  it should "not start on wrong inet address" in {
    val zkTestServer = new TestingServer(true)
    val storageOptions = StorageOptions()
    val serverBuilder = new ServerBuilder()
      .withServerStorageOptions(storageOptions)
      .withZookeeperOptions(ZookeeperOptions(endpoints = zkTestServer.getConnectString))
      .withBootstrapOptions(BootstrapOptions(host = "1270.0.0.1"))

    assertThrows[InvalidSocketAddress] {
      serverBuilder.build()
    }
    zkTestServer.close()
    FileUtils.deleteDirectory(new File(storageOptions.path + java.io.File.separatorChar + storageOptions.metadataDirectory))
    FileUtils.deleteDirectory(new File(storageOptions.path + java.io.File.separatorChar + storageOptions.dataDirectory))
    FileUtils.deleteDirectory(new File(storageOptions.path + java.io.File.separatorChar + storageOptions.commitLogRocksDirectory))
    FileUtils.deleteDirectory(new File(storageOptions.path + java.io.File.separatorChar + storageOptions.commitLogDirectory))
  }

  it should "not start on negative port value" in {
    val zkTestServer = new TestingServer(true)
    val storageOptions = StorageOptions()
    val serverBuilder = new ServerBuilder()
      .withServerStorageOptions(storageOptions)
      .withZookeeperOptions(ZookeeperOptions(endpoints = zkTestServer.getConnectString))
      .withBootstrapOptions(BootstrapOptions(port = Int.MinValue))

    assertThrows[InvalidSocketAddress] {
      serverBuilder.build()
    }

    FileUtils.deleteDirectory(new File(storageOptions.path + java.io.File.separatorChar + storageOptions.metadataDirectory))
    FileUtils.deleteDirectory(new File(storageOptions.path + java.io.File.separatorChar + storageOptions.dataDirectory))
    FileUtils.deleteDirectory(new File(storageOptions.path + java.io.File.separatorChar + storageOptions.commitLogRocksDirectory))
    FileUtils.deleteDirectory(new File(storageOptions.path + java.io.File.separatorChar + storageOptions.commitLogDirectory))
    zkTestServer.close()
  }

  it should "not start on port value exceeds 65535" in {
    val zkTestServer = new TestingServer(true)
    val storageOptions = StorageOptions()
    val serverBuilder = new ServerBuilder()
      .withServerStorageOptions(storageOptions)
      .withZookeeperOptions(ZookeeperOptions(endpoints = zkTestServer.getConnectString))
      .withBootstrapOptions(BootstrapOptions(port = 65536))

    assertThrows[InvalidSocketAddress] {
      serverBuilder.build()
    }
    zkTestServer.close()
    FileUtils.deleteDirectory(new File(storageOptions.path + java.io.File.separatorChar + storageOptions.metadataDirectory))
    FileUtils.deleteDirectory(new File(storageOptions.path + java.io.File.separatorChar + storageOptions.dataDirectory))
    FileUtils.deleteDirectory(new File(storageOptions.path + java.io.File.separatorChar + storageOptions.commitLogRocksDirectory))
    FileUtils.deleteDirectory(new File(storageOptions.path + java.io.File.separatorChar + storageOptions.commitLogDirectory))
  }

}
