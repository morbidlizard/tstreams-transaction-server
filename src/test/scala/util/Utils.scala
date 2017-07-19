package util

import java.io.File
import java.net.ServerSocket
import java.nio.file.Files
import java.util
import java.util.concurrent.{CountDownLatch, TimeUnit}

import com.bwsw.tstreamstransactionserver.netty.client.Client
import com.bwsw.tstreamstransactionserver.netty.client.api.TTSClient
import com.bwsw.tstreamstransactionserver.netty.server.db.zk.ZookeeperStreamRepository
import com.bwsw.tstreamstransactionserver.netty.server.{RocksReader, RocksWriter, TransactionServer}
import com.bwsw.tstreamstransactionserver.netty.server.storage.AllInOneRockStorage
import com.bwsw.tstreamstransactionserver.netty.server.transactionDataService.TransactionDataServiceImpl
import com.bwsw.tstreamstransactionserver.netty.server.transactionMetadataService.stateHandler.LastTransactionStreamPartition
import com.bwsw.tstreamstransactionserver.options.CommonOptions.ZookeeperOptions
import com.bwsw.tstreamstransactionserver.options.ServerOptions.{RocksStorageOptions, StorageOptions}
import com.bwsw.tstreamstransactionserver.options.{ClientBuilder, ServerOptions, SingleNodeServerBuilder}
import org.apache.bookkeeper.conf.ServerConfiguration
import org.apache.bookkeeper.meta.HierarchicalLedgerManagerFactory
import org.apache.bookkeeper.proto.BookieServer
import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import org.apache.curator.retry.RetryNTimes
import org.apache.curator.test.TestingServer
import org.apache.zookeeper.CreateMode
import org.apache.zookeeper.ZooDefs.Ids
import org.apache.zookeeper.data.ACL


object Utils {
  def uuid: String = java.util.UUID.randomUUID.toString

  private val sessionTimeoutMillis = 1000
  private val connectionTimeoutMillis = 1000


  def startZkServerAndGetIt: (TestingServer, CuratorFramework) = {
    val zkServer = new TestingServer(true)

    val zkClient = CuratorFrameworkFactory.builder
      .sessionTimeoutMs(sessionTimeoutMillis)
      .connectionTimeoutMs(connectionTimeoutMillis)
      .retryPolicy(new RetryNTimes(3, 100))
      .connectString(zkServer.getConnectString)
      .build()

    zkClient.start()
    zkClient.blockUntilConnected(connectionTimeoutMillis, TimeUnit.MILLISECONDS)

    (zkServer, zkClient)
  }


  private val zkLedgersRootPath = "/ledgers"
  private val zkBookiesAvailablePath = s"$zkLedgersRootPath/available"
  def startBookieServer(zkEndpoints: String, bookieNumber: Int): BookieServer = {

    def createBookieFolder() = {
      val path = Files.createTempDirectory(s"bookie")

      val bookieFolder =
        new File(path.toFile.getPath, "current")

      bookieFolder.mkdir()

      bookieFolder.getPath
    }

    def startBookie(): BookieServer = {
      val bookieFolder = createBookieFolder()

      val serverConfig = new ServerConfiguration()
        .setBookiePort(Utils.getRandomPort)
        .setZkServers(zkEndpoints)
        .setJournalDirName(bookieFolder)
        .setLedgerDirNames(Array(bookieFolder))
        .setAllowLoopback(true)

      serverConfig
        .setZkLedgersRootPath(zkLedgersRootPath)

      serverConfig.setLedgerManagerFactoryClass(
        classOf[HierarchicalLedgerManagerFactory]
      )

      val server = new BookieServer(serverConfig)
      server.start()
      server
    }
    startBookie()
  }

  def startZkServerBookieServerZkClient(serverNumber: Int): (TestingServer, CuratorFramework, Array[BookieServer]) = {
    val (zkServer, zkClient) = startZkServerAndGetIt

    zkClient.create()
      .creatingParentsIfNeeded()
      .withMode(CreateMode.PERSISTENT)
      .withACL(new util.ArrayList[ACL](Ids.OPEN_ACL_UNSAFE))
      .forPath(zkLedgersRootPath)

    zkClient.create()
      .withMode(CreateMode.PERSISTENT)
      .withACL(new util.ArrayList[ACL](Ids.OPEN_ACL_UNSAFE))
      .forPath(zkBookiesAvailablePath)

    val bookies = (0 until serverNumber).map(serverIndex =>
      startBookieServer(
        zkClient.getZookeeperClient.getCurrentConnectionString,
        serverIndex
      )
    ).toArray

    (zkServer, zkClient, bookies)
  }

  private val rand = scala.util.Random
  def getRandomStream =
    new com.bwsw.tstreamstransactionserver.rpc.StreamValue {
      override val name: String = rand.nextInt(10000).toString
      override val partitions: Int = rand.nextInt(10000)
      override val description: Option[String] = if (rand.nextBoolean()) Some(rand.nextInt(10000).toString) else None
      override val ttl: Long = Long.MaxValue
      override val zkPath: Option[String] = None
    }

  def getRandomPort: Int = {
    scala.util.Try {
      new ServerSocket(0)
    }.map { server =>
      val port = server.getLocalPort
      server.close()
      port
    }.get
  }

  def getTransactionServerBundle(zkClient: CuratorFramework): TransactionServerBundle = {
    val authOptions =
      com.bwsw.tstreamstransactionserver.options.ServerOptions.AuthenticationOptions()

    val dbPath = Files.createTempDirectory("tts").toFile

    val storageOptions =
      StorageOptions().copy(
        path = dbPath.getPath,
        streamZookeeperDirectory = s"/$uuid"
      )
    val rocksStorageOptions =
      RocksStorageOptions()

    val rocksStorage =
      new AllInOneRockStorage(
        storageOptions,
        rocksStorageOptions
      )

    val lastTransactionStreamPartition =
      new LastTransactionStreamPartition(
        rocksStorage.getRocksStorage
      )

    val zkStreamRepository =
      new ZookeeperStreamRepository(
        zkClient,
        storageOptions.streamZookeeperDirectory
      )

    val transactionDataServiceImpl =
      new TransactionDataServiceImpl(
        storageOptions,
        rocksStorageOptions,
        zkStreamRepository
      )

    val rocksWriter =
      new RocksWriter(
        rocksStorage,
        lastTransactionStreamPartition,
        transactionDataServiceImpl
      )

    val rocksReader =
      new RocksReader(
        rocksStorage,
        lastTransactionStreamPartition,
        transactionDataServiceImpl
      )

    val transactionServer =
      new TransactionServer(
        authOptions,
        zkStreamRepository,
        rocksWriter,
        rocksReader
      )

    new TransactionServerBundle(
      transactionServer,
      rocksStorage,
      transactionDataServiceImpl,
      storageOptions,
      rocksStorageOptions
    )
  }

  def startTransactionServer(builder: SingleNodeServerBuilder): ZkSeverAndTransactionServer = {
    val zkTestServer = new TestingServer(true)
    val transactionServer = builder
      .withZookeeperOptions(
        builder.getZookeeperOptions.copy(endpoints = zkTestServer.getConnectString)
      )
      .withBootstrapOptions(
        builder.getBootstrapOptions.copy(bindPort = getRandomPort)
      )
      .build()

    val latch = new CountDownLatch(1)
    new Thread(() => {
      transactionServer.start(latch.countDown())
    }).start()

    latch.await(3000, TimeUnit.SECONDS)
    ZkSeverAndTransactionServer(zkTestServer, transactionServer)
  }

  def startTransactionServerAndClient(zkClient: CuratorFramework,
                                      serverBuilder: SingleNodeServerBuilder,
                                      clientBuilder: ClientBuilder): ZkSeverTxnServerTxnClient = {
    val dbPath = Files.createTempDirectory("tts").toFile
    val zKCommonMasterPrefix = s"/$uuid"


    val updatedBuilder = serverBuilder
      .withZookeeperOptions(
        serverBuilder.getZookeeperOptions.copy(
          endpoints = zkClient.getZookeeperClient.getCurrentConnectionString,
          prefix = zKCommonMasterPrefix
        )
      )
      .withServerStorageOptions(
        serverBuilder.getStorageOptions.copy(
          path = dbPath.getPath,
          streamZookeeperDirectory = s"/$uuid")
      )
      .withServerRoleOptions(
        serverBuilder.getServerRoleOptions.copy(commonMasterElectionPrefix = s"/$uuid")
      )
      .withBootstrapOptions(
        serverBuilder.getBootstrapOptions.copy(bindPort = getRandomPort)
      )


    val transactionServer = updatedBuilder.build()


    val latch = new CountDownLatch(1)
    new Thread(() => {
      transactionServer.start(latch.countDown())
    }).start()

    if (!latch.await(5000, TimeUnit.SECONDS))
      throw new IllegalStateException()

    val client = new ClientBuilder()
      .withZookeeperOptions(
        ZookeeperOptions(
          endpoints = zkClient.getZookeeperClient.getCurrentConnectionString,
          prefix = zKCommonMasterPrefix
        )
      )
      .build()

    new ZkSeverTxnServerTxnClient(transactionServer, client, updatedBuilder)
  }

  def startTransactionServerAndClient(zkClient: CuratorFramework,
                                      serverBuilder: SingleNodeServerBuilder,
                                      clientBuilder: ClientBuilder,
                                      clientsNumber: Int): ZkSeverTxnServerTxnClients = {
    val dbPath = Files.createTempDirectory("tts").toFile

    val streamRepositoryPath =
      s"/$uuid"

    val zkConnectionString =
      zkClient.getZookeeperClient.getCurrentConnectionString

    val port =
      getRandomPort

    val updatedBuilder = serverBuilder
      .withZookeeperOptions(
        serverBuilder.getZookeeperOptions.copy(
          endpoints = zkConnectionString
        )
      )
      .withServerStorageOptions(
        serverBuilder.getStorageOptions.copy(
          path = dbPath.getPath,
          streamZookeeperDirectory = streamRepositoryPath)
      )
      .withBootstrapOptions(
        serverBuilder.getBootstrapOptions.copy(bindPort = port)
      )


    val transactionServer = updatedBuilder.build()


    val latch = new CountDownLatch(1)
    new Thread(() => {
      transactionServer.start(latch.countDown())
    }).start()

    if (!latch.await(5000, TimeUnit.SECONDS))
      throw new IllegalStateException()


    val clients: Array[TTSClient] = Array.fill(clientsNumber) {
      new ClientBuilder()
        .withZookeeperOptions(
          ZookeeperOptions(
            endpoints = zkConnectionString
          )
        ).build()
    }

    new ZkSeverTxnServerTxnClients(transactionServer, clients, updatedBuilder)
  }



}
