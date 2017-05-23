package ut

import java.io.File
import java.util.concurrent.PriorityBlockingQueue

import com.bwsw.commitlog.filesystem.{CommitLogCatalogue, CommitLogStorage}
import com.bwsw.tstreamstransactionserver.configProperties.ServerExecutionContext
import com.bwsw.tstreamstransactionserver.netty.server.db.zk.StreamDatabaseZK
import com.bwsw.tstreamstransactionserver.netty.server.{CommitLogQueueBootstrap, TransactionServer}
import com.bwsw.tstreamstransactionserver.options.ServerOptions.{AuthOptions, RocksStorageOptions, StorageOptions}
import it.Utils.startZkServerAndGetIt
import org.apache.commons.io.FileUtils
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

import scala.collection.mutable.ArrayBuffer

class CommitLogQueueBootstrapTestSuite extends FlatSpec with Matchers with BeforeAndAfterAll {
  //arrange
  val authOptions = AuthOptions()
  val rocksStorageOptions = RocksStorageOptions()
  val executionContext = new ServerExecutionContext(2, 2)
  val storageOptions = StorageOptions(new StringBuffer().append("target").append(File.separatorChar).append("clqb").toString)


  private val path = "/tts/test_path"
  private lazy val (zkServer, zkClient) = startZkServerAndGetIt
  private lazy val streamDatabaseZK = new StreamDatabaseZK(zkClient, path)
  private lazy val transactionServer = new TransactionServer(
    executionContext = executionContext,
    authOpts = authOptions,
    storageOpts = storageOptions,
    rocksStorageOpts = rocksStorageOptions,
    streamDatabaseZK
  )

  val commitLogCatalogue = new CommitLogCatalogue(storageOptions.path + java.io.File.separatorChar + storageOptions.commitLogDirectory)
  val commitLogQueueBootstrap = new CommitLogQueueBootstrap(10, commitLogCatalogue, transactionServer)



  "fillQueue" should "return an empty queue if there are no commit log files in a storage directory" in {
    //act
    val emptyQueue = commitLogQueueBootstrap.fillQueue()

    //assert
    emptyQueue shouldBe empty
  }

  "fillQueue" should "return a queue of a size that equals to a number of commit log files are in a storage directory" in {
    //arrange
    val numberOfFiles = 10
    createCommitLogFiles(numberOfFiles)

    //act
    val nonemptyQueue = commitLogQueueBootstrap.fillQueue()

    //assert
    nonemptyQueue should have size numberOfFiles
  }

  "fillQueue" should "return a queue with the time ordered commit log files" in {
    //arrange
    val numberOfFiles = 1
    createCommitLogFiles(numberOfFiles)
    createCommitLogFiles(numberOfFiles)
    createCommitLogFiles(numberOfFiles)

    //act
    val orderedQueue = commitLogQueueBootstrap.fillQueue()
    val orderedFiles = getOrderedFiles(orderedQueue)

    //assert
    orderedFiles shouldBe sorted
  }

  override def afterAll = {
    transactionServer.stopAccessNewTasksAndAwaitAllCurrentTasksAreCompletedAndCloseDatabases()
    FileUtils.deleteDirectory(new File(storageOptions.path))
    zkClient.close()
    zkServer.close()
  }

  private def createCommitLogFiles(number: Int) = {
    val commitLogCatalogueByDate = new CommitLogCatalogue(storageOptions.path + java.io.File.separatorChar + storageOptions.commitLogDirectory)

    (0 until number).foreach(fileNamePrefix => {
      commitLogCatalogueByDate.createFile(fileNamePrefix.toString)
    })
  }

  private def getOrderedFiles(orderedQueue: PriorityBlockingQueue[CommitLogStorage]): ArrayBuffer[CommitLogStorage] = {
    val orderedFiles = ArrayBuffer[CommitLogStorage]()
    var path = orderedQueue.poll()
    while (path != null) {
      orderedFiles += path
      path = orderedQueue.poll()
    }

    orderedFiles
  }
}
